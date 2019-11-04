package cn.shijinshi.redis.control.controller;

import cn.shijinshi.redis.common.param.Cluster;
import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.common.param.Node;
import cn.shijinshi.redis.common.protocol.RedisProbe;
import cn.shijinshi.redis.common.util.ClusterUtil;
import cn.shijinshi.redis.common.util.SimpleThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Redis是集群模式的情况。
 *
 * 这种情况就比较麻烦了，目前并没有很好的手段能够及时知道Redis集群的节点变化情况。
 * 在最初的设计中，如果Broker检测到了来自于Redis节点的 "-MOVED ..." 报文，则认为
 * 集群已发生变化，但是这种并不是很稳定。
 *
 * 那么现在的做法是，定时去Redis集群获取当前集群信息，和之前的做对比，如果不一致，
 * 则认为是Redis集群发生了变化。
 *
 * 但是，如果Redis集群节点数量很多的话，会产生更多的损耗。
 * 将来可能会有优化。
 *
 * @author Gui Jiahai
 */
public class RedisClusterDetector implements RedisDetector, Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RedisClusterDetector.class);

    private final RedisDetectorCallback callback;

    private ScheduledExecutorService scheduledExecutor;

    private boolean started = false;

    private final List<HostAndPort> rawNodes;

    /**
     * 根据原有的地址，获取Redis集群所有节点地址。
     * 然后，轮询的从Redis节点中获取集群信息。
     * 这样，可以防止对单节点有压力。
     */
    private final List<HostAndPort> probeNodes = new ArrayList<>();
    private final Map<HostAndPort, RedisProbe> probeCache = new HashMap<>();
    private int index = 0;

    private volatile Cluster preCluster;

    public RedisClusterDetector(List<HostAndPort> nodes, RedisDetectorCallback callback) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty");
        }
        rawNodes = Collections.unmodifiableList(new ArrayList<>(nodes));
        this.callback = Objects.requireNonNull(callback);
    }

    @Override
    public synchronized void start() {
        if (!started) {
            started = true;

            scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
                    new SimpleThreadFactory("redis-cluster-detector-scheduler", true));
            scheduledExecutor.scheduleWithFixedDelay(this, 0, 2, TimeUnit.SECONDS);
        }
    }

    @Override
    public synchronized void run() {
        if (!started) {
            return;
        }

        try {

            if (this.probeNodes.isEmpty()) {
                for (HostAndPort node : this.rawNodes) {
                    Cluster cluster = discover(node);
                    if (cluster != null) {
                        reduce(cluster);
                        this.notify(cluster);
                        return;
                    }
                }
                logger.error("Failed to get redis cluster info with raw nodes: {}", rawNodes);
                return;
            }

            int len = this.probeNodes.size();
            while (len -- > 0 && !Thread.interrupted()) {
                int idx = (index ++) % this.probeNodes.size();
                HostAndPort node = this.probeNodes.get(idx);
                Cluster cluster = discover(node);
                if (cluster != null) {
                    reduce(cluster);
                    this.notify(cluster);
                    return;
                }
            }

            logger.error("Failed to get redis cluster info with probe nodes: {}", probeNodes);

        } catch (Throwable t) {

            logger.error("Failed to get redis cluster info with probe nodes: {}", probeNodes, t);
        }

    }

    private Cluster discover(HostAndPort node) {
        if (!probeCache.containsKey(node)) {
            try {
                RedisProbe probe = new RedisProbe(node);
                probeCache.put(node, probe);
            } catch (IOException e) {
                logger.error("Failed to create RedisProbe with node: {}", node, e);
                return null;
            }
        }

        RedisProbe probe = probeCache.get(node);
        try {
            return probe.discover();
        } catch (IOException e) {
            logger.warn("Failed to discover cluster info with node: {}", node, e);
            Closeable c = probeCache.remove(node);
            if (c != null) {
                try {
                    c.close();
                } catch (IOException ignored) {}
            }
            return null;
        }
    }

    private void reduce(Cluster cluster) {
        this.probeNodes.clear();
        for (Node node : cluster.getNodes()) {
            probeNodes.add(node.getRedis());
            if (node.getSlaves() != null) {
                probeNodes.addAll(node.getSlaves());
            }
        }

        Iterator<Map.Entry<HostAndPort, RedisProbe>> iterator = this.probeCache.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<HostAndPort, RedisProbe> next = iterator.next();
            if (!this.probeNodes.contains(next.getKey())) {
                iterator.remove();

                Closeable c = next.getValue();
                if (c != null) {
                    try {
                        c.close();
                    } catch (IOException ignored) {}
                }

                logger.info("Release redis probe: {}", next.getKey());
            }
        }
    }

    private void notify(Cluster cluster) {
        //对nodes进行排序，便于后续进行对比
        ClusterUtil.sort(cluster);
        if (!ClusterUtil.equals(preCluster, cluster, false, true)) {
            preCluster = cluster;
            this.callback.redisChanged(cluster);
        }
    }

    private boolean diff(Cluster pre, Cluster now) {
        if (pre == null) {
            return true;
        }

        if (pre.getNodes().size() != now.getNodes().size()) {
            return true;
        }

        for (int i = 0; i < now.getNodes().size(); i ++) {
            Node p = pre.getNodes().get(i);
            Node n = now.getNodes().get(i);

            if (!p.getRedis().equals(n.getRedis())) {
                return true;
            }
            if (!p.getRanges().equals(n.getSlaves())) {
                return true;
            }
        }
        return false;
    }

    private void clearProbe() {
        for (RedisProbe probe : probeCache.values()) {
            try {
                probe.close();
            } catch (IOException ignored) {}
        }
        probeCache.clear();
    }

    @Override
    public synchronized void stop() {
        if (started) {
            started = false;
            scheduledExecutor.shutdownNow();
            scheduledExecutor = null;
            clearProbe();
        }
    }

}
