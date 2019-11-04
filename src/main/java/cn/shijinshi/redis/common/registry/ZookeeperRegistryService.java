package cn.shijinshi.redis.common.registry;

import cn.shijinshi.redis.common.util.ExecutorUtil;
import cn.shijinshi.redis.common.util.SimpleThreadFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * 依赖zookeeper实现的注册服务。
 *
 * 这里重点说明以下重试机制。
 * 当发生重试时，要重点关注两个问题，
 * 1、临时节点是否还在，如果不在了，应该要重新创建
 * 2、监听器是否能正常运行，如果不能，则应该重新添加监听器
 *
 * 对于第一点，由于本类不了解用户的需求，所以操作很困难。
 * 这里强调，用户需要自己去监听注册中心的状态，然后判断临时节点是否存在；
 *
 * 对于第二点，创建了两个监听器重试容器 failedChildListeners 和 failedDataListeners，
 * 如果出现重连或者添加失败，会自动将监听器放入重试容器中，scheduler会定时去容器中
 * 获取失败的监听器，重新添加
 *
 * @author Gui Jiahai
 */
public class ZookeeperRegistryService implements RegistryService {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperRegistryService.class);

    private static final long retryIntervalMs = 3000;

    private final CuratorFramework client;

    private final Set<StateListener> stateListeners = new CopyOnWriteArraySet<>();
    private final ConcurrentMap<String, ConcurrentMap<ChildListener, ChildWatcher>> childListeners = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConcurrentMap<DataListener, DataWatcher>> dataListeners = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, Set<ChildListener>> failedChildListeners = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<DataListener>> failedDataListeners = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
            new SimpleThreadFactory("zookeeper-registry-scheduler", true));

    public ZookeeperRegistryService(String url) {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
                .connectString(url)
                .defaultData(new byte[0])
                .sessionTimeoutMs(1000)
                .retryPolicy(new RetryNTimes(1, 1000))
                .connectionTimeoutMs(5000);
        client = builder.build();

        client.getConnectionStateListenable().addListener((client, newState) -> {
            if (newState == ConnectionState.LOST) {
                ZookeeperRegistryService.this.stateChanged(StateListener.State.DISCONNECTED);
            } else if (newState == ConnectionState.CONNECTED) {
                ZookeeperRegistryService.this.stateChanged(StateListener.State.CONNECTED);
            } else if (newState == ConnectionState.RECONNECTED) {
                ZookeeperRegistryService.this.stateChanged(StateListener.State.RECONNECTED);
            }
        });
        client.start();
        try {
            boolean connected = client.blockUntilConnected(5000, TimeUnit.MILLISECONDS);
            if (!connected) {
                throw new IllegalStateException("ZookeeperProperties not connected");
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }

        scheduledExecutor.scheduleWithFixedDelay(() -> {
            if (!failedChildListeners.isEmpty()) {
                for (Map.Entry<String, Set<ChildListener>> entry : failedChildListeners.entrySet()) {
                    String path = entry.getKey();
                    Set<ChildListener> set = entry.getValue();

                    if (set != null && !set.isEmpty()) {
                        Iterator<ChildListener> iterator = set.iterator();
                        while (iterator.hasNext()) {
                            try {
                                addChildListener(path, iterator.next());
                                iterator.remove();
                            } catch (RuntimeException ignored) {
                            }
                        }
                    }
                }
            }

            if (!failedDataListeners.isEmpty()) {
                for (Map.Entry<String, Set<DataListener>> entry : failedDataListeners.entrySet()) {
                    String path = entry.getKey();
                    Set<DataListener> set = entry.getValue();

                    if (set != null && !set.isEmpty()) {
                        Iterator<DataListener> iterator = set.iterator();
                        while (iterator.hasNext()) {
                            try {
                                addDataListener(path, iterator.next());
                                iterator.remove();
                            } catch (RuntimeException ignored) {
                            }
                        }
                    }
                }
            }

        }, retryIntervalMs, retryIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void stateChanged(StateListener.State state) {
        if (state == StateListener.State.RECONNECTED) {
            recover();
        }

        if (stateListeners.size() > 0) {
            for (StateListener listener : stateListeners) {
                try {
                    listener.stateChanged(state);
                } catch (Exception e) {
                    logger.error("stateChanged error:[{}]", state.name(), e);
                }
            }
        }
    }

    private void recover() {
        if (!childListeners.isEmpty()) {
            for (Map.Entry<String, ConcurrentMap<ChildListener, ChildWatcher>> entry : childListeners.entrySet()) {
                String path = entry.getKey();
                for (ChildListener listener : entry.getValue().keySet())
                    try {
                        addChildListener(path, listener);
                    } catch (RuntimeException ignored) {
                    }
            }
        }
        if (!dataListeners.isEmpty()) {
            for (Map.Entry<String, ConcurrentMap<DataListener, DataWatcher>> entry : dataListeners.entrySet()) {
                String path = entry.getKey();
                for (DataListener listener : entry.getValue().keySet()) {
                    try {
                        addDataListener(path, listener);
                    } catch (RuntimeException ignored) {
                    }
                }
            }
        }
    }

    @Override
    public String create(String path, boolean ephemeral, boolean sequential) throws RegistryException {
        CreateMode mode;
        if (ephemeral) {
            mode = sequential ? CreateMode.EPHEMERAL_SEQUENTIAL : CreateMode.EPHEMERAL;
        } else {
            mode = sequential ? CreateMode.PERSISTENT_SEQUENTIAL : CreateMode.PERSISTENT;
        }
        try {
            return client.create().creatingParentsIfNeeded().withMode(mode).forPath(path);
        } catch (KeeperException.NodeExistsException e) {
            logger.warn("The node already exists, path: {}, mode: {}", path, mode.name());
            return null;
        } catch (Exception e) {
            throw new RegistryException("Failed to create path: " + path, e);
        }
    }

    @Override
    public void delete(String path) throws RegistryException {
        try {

            client.delete().forPath(path);

            //delete child listener
            ConcurrentMap<ChildListener, ChildWatcher> childListeners = this.childListeners.remove(path);
            if (childListeners != null) {
                for (Map.Entry<ChildListener, ChildWatcher> entry : childListeners.entrySet()) {
                    entry.getValue().unwatch();
                }
            }

            //delete data listener
            ConcurrentMap<DataListener, DataWatcher> dataListeners = this.dataListeners.remove(path);
            if (dataListeners != null) {
                for (Map.Entry<DataListener, DataWatcher> entry : dataListeners.entrySet()) {
                    entry.getValue().unwatch();
                }
            }

        } catch (KeeperException.NoNodeException e) {
            logger.warn("The node does not exists, path: {}", path);
        } catch (Exception e) {
            throw new RegistryException("Failed to delete path: " + path, e);
        }
    }

    @Override
    public List<String> getChildren(String path) throws RegistryException {
        try {
            return client.getChildren().forPath(path);
        } catch (KeeperException.NoNodeException e) {
            logger.warn("The node does not exists, path: {}", path);
            return null;
        } catch (Exception e) {
            throw new RegistryException("Failed to get children with path: " + path, e);
        }
    }

    @Override
    public void addChildListener(String path, ChildListener listener) {
        ConcurrentMap<ChildListener, ChildWatcher> map = childListeners.get(path);
        if (map == null) {
            childListeners.putIfAbsent(path, new ConcurrentHashMap<>());
            map = childListeners.get(path);
        }

        ChildWatcher watcher = map.get(listener);
        if (watcher == null) {
            map.putIfAbsent(listener, new ChildWatcher(listener));
            watcher = map.get(listener);
        }

        try {
            List<String> list = client.getChildren().usingWatcher(watcher).forPath(path);
            listener.childChanged(path, list);
        } catch (KeeperException.NoNodeException e) {
            logger.error("Failed to add child listener, cause path[{}] does not exists", path, e);
            throw new IllegalStateException(e);
        } catch (Exception e) {
            Set<ChildListener> set = failedChildListeners.get(path);
            if (set == null) {
                failedChildListeners.putIfAbsent(path, Collections.synchronizedSet(new HashSet<>()));
                set = failedChildListeners.get(path);
            }
            set.add(listener);
        }
    }

    @Override
    public void removeChildListener(String path, ChildListener listener) {
        Set<ChildListener> set = failedChildListeners.get(path);
        if (set != null) {
            set.remove(listener);
        }

        ConcurrentMap<ChildListener, ChildWatcher> map = this.childListeners.get(path);
        if (map != null) {
            ChildWatcher watcher = map.remove(listener);
            if (watcher != null) {
                watcher.unwatch();
            }
        }
    }

    @Override
    public byte[] getData(String path) throws RegistryException {
        try {
            return client.getData().forPath(path);
        } catch (Exception e) {
            throw new RegistryException("Failed to get data with path: " + path, e);
        }
    }

    @Override
    public void setData(String path, byte[] data) throws RegistryException {
        try {
            client.setData().forPath(path, data);
        } catch (Exception e) {
            throw new RegistryException("Failed to get data with path: " + path, e);
        }
    }

    @Override
    public void addDataListener(String path, DataListener listener) {
        ConcurrentMap<DataListener, DataWatcher> map = dataListeners.get(path);
        if (map == null) {
            dataListeners.putIfAbsent(path, new ConcurrentHashMap<>());
            map = dataListeners.get(path);
        }

        DataWatcher watcher = map.get(listener);
        if (watcher == null) {
            map.putIfAbsent(listener, new DataWatcher(listener));
            watcher = map.get(listener);
        }

        try {
            byte[] data = client.getData().usingWatcher(watcher).forPath(path);
            listener.dataChanged(path, data);
        } catch (KeeperException.NoNodeException e) {
            logger.error("Failed to add data listener, cause path[{}] does not exists", path, e);
            throw new IllegalStateException(e);
        } catch (Exception e) {
            Set<DataListener> set = failedDataListeners.get(path);
            if (set == null) {
                failedDataListeners.putIfAbsent(path, Collections.synchronizedSet(new HashSet<>()));
                set = failedDataListeners.get(path);
            }
            set.add(listener);
        }
    }

    @Override
    public void removeDataListener(String path, DataListener listener) {
        Set<DataListener> set = failedDataListeners.get(path);
        if (set != null) {
            set.remove(listener);
        }

        ConcurrentMap<DataListener, DataWatcher> map = this.dataListeners.get(path);
        if (map != null) {
            DataWatcher watcher = map.remove(listener);
            if (watcher != null) {
                watcher.unwatch();
            }
        }
    }

    @Override
    public void addStateListener(StateListener listener) {
        stateListeners.add(listener);
    }

    @Override
    public void removeStateListener(StateListener listener) {
        stateListeners.remove(listener);
    }

    @Override
    public boolean checkExists(String path) throws RegistryException {
        try {
            if (client.checkExists().forPath(path) != null) {
                return true;
            }
        } catch (Exception e) {
            throw new RegistryException("Failed to check exists with path: " + path, e);
        }
        return false;
    }

    @Override
    public boolean isConnected() {
        return client.getZookeeperClient().isConnected();
    }

    @Override
    public void close() {
        ExecutorUtil.shutdown(scheduledExecutor, 0, TimeUnit.MILLISECONDS);
        if (client != null) {
            client.close();
        }
    }

    class ChildWatcher implements CuratorWatcher {

        private volatile ChildListener listener;

        ChildWatcher(ChildListener listener) {
            this.listener = Objects.requireNonNull(listener);
        }

        @Override
        public void process(WatchedEvent event) throws Exception {
            String path;
            if (listener != null && (path = event.getPath()) != null) {
                try {
                    listener.childChanged(path, path.isEmpty() ? Collections.emptyList() : client.getChildren().usingWatcher(this).forPath(path));
                } catch (KeeperException.NoNodeException ignored) {
                }
            }
        }

        public void unwatch() {
            this.listener = null;
        }
    }

    class DataWatcher implements CuratorWatcher {

        private volatile DataListener listener;

        DataWatcher(DataListener listener) {
            this.listener = Objects.requireNonNull(listener);
        }

        @Override
        public void process(WatchedEvent event) throws Exception {
            String path;
            if (listener != null && (path = event.getPath()) != null) {
                try {
                    listener.dataChanged(path, client.getData().usingWatcher(this).forPath(path));
                } catch (KeeperException.NoNodeException ignored) {
                }
            }
        }

        public void unwatch() {
            this.listener = null;
        }
    }
}
