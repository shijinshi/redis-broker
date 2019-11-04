package cn.shijinshi.redis.control.broker;

import cn.shijinshi.redis.common.Constants;
import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.common.param.Cluster;
import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.common.param.Node;
import cn.shijinshi.redis.common.registry.RegistryException;
import cn.shijinshi.redis.common.registry.RegistryService;
import cn.shijinshi.redis.common.registry.StateListener;
import cn.shijinshi.redis.common.util.ExecutorUtil;
import cn.shijinshi.redis.common.util.SimpleThreadFactory;
import cn.shijinshi.redis.control.rpc.Answer;
import cn.shijinshi.redis.control.rpc.Indication;
import cn.shijinshi.redis.control.rpc.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.concurrent.*;

/**
 * Broker是一个逻辑概念，每个节点进程都可以统称为Broker
 *
 * Broker管理着程序的模块，启动、停止等功能。
 * 大概可以分为三个功能：
 * 1、{@code FixedForwardLauncher} 用于将客户端的请求发送到Redis中，
 * 然后将redis响应发送回客户端。并且将部分请求存储到磁盘日志上。
 * 2、{@code RelatedSyncLauncher} 同步功能，读取磁盘日志上的请求，
 * 然后转发到另一个Redis集群中，使两个Redis集群的数据同步。
 * 3、{@code RelatedReplLauncher} 复制功能，可以将一个Redis集群的数据
 * 全量复制到另一个Redis集群。
 * 4、接收来自Controller的命令(Indication)，并将命令交由合适的模块处理。
 *
 * 这里涉及到三个Starter是独立的模块。默认情况下，Broker具有三个模块。
 * 但是，可以通过删减模块，使得Broker只具有特定的功能，例如复制功能。
 *
 *
 * 架构设计请参考 {@link cn.shijinshi.redis.control.controller.BaseController}
 *
 * @author Gui Jiahai
 */
public class Broker implements StateListener {

    private static final Logger logger = LoggerFactory.getLogger(Broker.class);

    private static final long DEFAULT_RETRY_MS = 3000;

    private final RegistryService registryService;
    private final BrokerProperties properties;
    private ScheduledExecutorService scheduledExecutor;
    private long retryMs;

    private final Set<NodeListener> listeners = new CopyOnWriteArraySet<>();

    private volatile ScheduledFuture<?> registerFuture;
    private volatile Cluster cluster;
    private volatile String registeredPath;
    private volatile long epoch;
    private volatile boolean master;

    private List<Launcher> launchers;

    public Broker(RegistryService registryService, BrokerProperties properties) {
        this.registryService = Objects.requireNonNull(registryService);
        this.properties = Objects.requireNonNull(properties);
        setRetryMs(DEFAULT_RETRY_MS);
    }

    @Autowired
    public void setLaunchers(List<Launcher> launchers) {
        this.launchers = new ArrayList<>(launchers);
    }

    public void setRetryMs(long retryMs) {
        if (retryMs <= 0) {
            throw new IllegalArgumentException("retryMs must be greater than 0");
        }
        this.retryMs = retryMs;
    }

    public void init() {
        if (this.launchers == null || this.launchers.isEmpty()) {
            throw new IllegalStateException("The launchers must not be empty");
        }

        for (Launcher launcher : launchers) {
            launcher.start();
        }

        try {
            if (!this.registryService.checkExists(properties.getBrokersPath())) {
                this.registryService.create(properties.getBrokersPath(), false, false);
            }
        } catch (RegistryException e) {
            throw new IllegalStateException(e);
        }

        registryService.addStateListener(this);
        registerBroker();
    }

    @Override
    public void stateChanged(State state) {
        if (state == State.RECONNECTED) {
            registerBroker();
        }
    }

    /**
     * 将Broker注册到注册中心，便于Controller发现，协调工作。
     * 如果注册失败，则以后都定时去重试。
     */
    private synchronized void registerBroker() {
        if (this.scheduledExecutor == null) {
            this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
                    new SimpleThreadFactory("broker-scheduler", true));
        }

        String path = properties.getBrokersPath() + Constants.PATH_SEPARATOR
                + HostAndPort.create(properties.getAddress(), properties.getPort()).format() + "-";
        if (registerFuture == null) {
            registerFuture = scheduledExecutor.scheduleWithFixedDelay(() -> {
                try {
                    if (registeredPath == null || !registryService.checkExists(registeredPath)) {
                        /*
                            注册的节点带有序号，用于选举Controller。
                            即序号最小的节点代表的Broker，会被选举为Controller
                         */
                        String s = registryService.create(path, true, true);
                        if (s != null) {
                            registeredPath = s;
                        }
                    }

                    ScheduledFuture<?> f = registerFuture;
                    if (f != null) {
                        registerFuture = null;
                        f.cancel(false);
                    }

                } catch (Exception e) {
                    logger.warn("Failed to register broker path: {}, it will be retried in {} milliseconds", path, DEFAULT_RETRY_MS, e);
                }
            }, 0, retryMs, TimeUnit.MILLISECONDS);
        }
    }

    private void unregisterBroker() {
        logger.info("unregister broker from registry service");
        registryService.removeStateListener(this);
        if (registeredPath != null) {
            try {
                registryService.delete(registeredPath);
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * 每当Controller更新BrokerCluster，就会发送到各个Broker。
     * BrokerCluster包含了所有Broker负责的工作任务，即broker负责的Redis节点和slot信息
     */
    private void refresh(Cluster cluster) {
        this.cluster = cluster;

        boolean master = false;
        HostAndPort redis = null;
        if (cluster != null) {
            HostAndPort localAddress = HostAndPort.create(properties.getAddress(), properties.getPort());
            for (Node node : cluster.getNodes()) {
                if (localAddress.equals(node.getBroker())) {
                    master = true;
                    redis = node.getRedis();
                    break;
                }

                if (node.getSlaves() != null && node.getSlaves().contains(localAddress)) {
                    redis = node.getRedis();
                    break;
                }
            }
        }

        for (NodeListener listener : listeners) {
            try {
                listener.nodeChanged(redis);
            } catch (Throwable t) {
                logger.error("Failed to nodeChanged", t);
            }
        }

        this.master = master;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public boolean isMaster() {
        return master;
    }

    /**
     * 处理来自Controller的Indication
     */
    public synchronized Answer answer(Indication indication) {

        /*
          如果当前的epoch大于indication.epoch时，
          说明该indication是由old_controller发出的
          否则，获取new_controller的epoch
         */
        if (indication.getEpoch() < epoch) {
            return Answer.expire();
        } else {
            this.epoch = indication.getEpoch();
        }

        if (indication.getType() == Type.PING) {
            return Answer.ok();
        } else if (indication.getType() == Type.CLUSTER) {
            refresh(indication.getCluster());
            return Answer.ok();
        } else {
            for (Launcher launcher : launchers) {
                if (launcher.support(indication)) {
                    return launcher.apply(indication);
                }
            }
        }

        return Answer.bad("Cannot process indication with type: " + indication.getType());
    }

    public void addListener(NodeListener listener) {
        listeners.add(listener);
    }

    @SuppressWarnings("unused")
    public void removeListener(NodeListener listener) {
        listeners.remove(listener);
    }

    public synchronized void destroy() {
        logger.info("Destroy broker ...");
        listeners.clear();
        ExecutorUtil.shutdown(scheduledExecutor, 10, TimeUnit.SECONDS);
        unregisterBroker();
        for (Launcher launcher : launchers) {
            launcher.stop();
        }
    }

}
