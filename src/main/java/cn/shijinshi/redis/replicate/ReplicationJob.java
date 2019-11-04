package cn.shijinshi.redis.replicate;

import cn.shijinshi.redis.common.prop.RedisProperties;
import cn.shijinshi.redis.common.param.Cluster;
import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.common.param.Node;
import cn.shijinshi.redis.common.protocol.RedisProbe;
import cn.shijinshi.redis.common.adapt.ReplicatorAdaptor;
import cn.shijinshi.redis.common.adapt.ReplicatorAdaptorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * 将 source redis (cluster) 的数据复制到 target redis (cluster)。
 * 如果 source redis 是单节点模式，则只用复制 source redis 即可。
 * 如果是集群模式， 则根据提供的地址， 获取所有的master节点，
 * 然后，将master节点上的数据复制到target redis (cluster)。
 *
 * @author Gui Jiahai
 */
public class ReplicationJob extends Thread implements Observer<HostAndPort> {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationJob.class);

    private final List<ReplicationTask> tasks = new ArrayList<>();

    private final Observer<ReplicationJob> observer;
    private final RedisProperties source;
    private final RedisProperties target;

    private final Set<HostAndPort> inCompletionSet = new HashSet<>();
    private final Map<HostAndPort, Boolean> completedMap = new HashMap<>();

    public ReplicationJob(RedisProperties source, RedisProperties target, Observer<ReplicationJob> observer) {
        super("replication-job-thread");
        setDaemon(true);

        this.source = source;
        this.target = target;

        this.observer = observer;
    }

    @Override
    public void run() {

        try (ReplicatorAdaptor adaptor = ReplicatorAdaptorFactory.create(target)) {

            Set<HostAndPort> nodes = getRedisNodes(this.source);
            if (nodes == null || nodes.isEmpty()) {
                logger.error("Cannot get any redis nodes from source: {}", this.source);
                if (this.observer != null) {
                    this.observer.onError(this);
                }
                return;
            }

            logger.info("Got redis nodes: {}, and start all replication task", nodes);

            for (HostAndPort node : nodes) {
                ReplicationTask task = new ReplicationTask(node, adaptor, this);
                tasks.add(task);
                task.start();
            }

            for (ReplicationTask task : tasks) {
                try {
                    task.join();
                } catch (InterruptedException e) {
                    logger.info("{} should not be interrupted", Thread.currentThread());
                }
            }

        } catch (Throwable t) {
            logger.error("Replication task error", t);
            if (this.observer != null) {
                this.observer.onError(this);
            }
        }
    }

    @Override
    public synchronized void onNext(HostAndPort hostAndPort) {
        logger.info("onNext: {}", hostAndPort);
        if (inCompletionSet.add(hostAndPort)) {
            if (inCompletionSet.size() == tasks.size()) {
                logger.info("ReplicationJob is in-completion");
                if (this.observer != null) {
                    this.observer.onNext(this);
                }
            }
        }
    }

    @Override
    public synchronized void onCompleted(HostAndPort hostAndPort) {
        logger.info("onCompleted: {}", hostAndPort);

        onNext(hostAndPort);
        completedMap.put(hostAndPort, Boolean.TRUE);
        done();
    }

    @Override
    public synchronized void onError(HostAndPort hostAndPort) {
        logger.info("onError: {}", hostAndPort);

        onNext(hostAndPort);
        completedMap.put(hostAndPort, Boolean.FALSE);
        done();
    }

    private void done() {
        if (completedMap.size() == tasks.size()) {
            boolean result = true;
            StringBuilder builder = new StringBuilder("Replication result:");
            for (Map.Entry<HostAndPort, Boolean> entry : completedMap.entrySet()) {
                builder.append(entry.getKey().format())
                        .append(',');
                if (!entry.getValue()) {
                    result = false;
                }
            }
            builder.deleteCharAt(builder.length() - 1);
            String s = builder.toString();

            logger.info("Replication completed, result: {}, message: {}", result, s);
            if (this.observer != null) {
                if (result) {
                    this.observer.onCompleted(this);
                } else {
                    this.observer.onError(this);
                }
            }
        }
    }

    public synchronized void completeWithTick(byte[] tick) {
        for (ReplicationTask task : tasks) {
            task.completeWithTick(tick);
        }
    }

    @Override
    public void interrupt() {
        logger.info("Interrupt replication job");

        for (ReplicationTask task : tasks) {
            if (task.isAlive()) {
                task.interrupt();
            }
        }
    }

    private Set<HostAndPort> getRedisNodes(RedisProperties redis) {
        if (redis.getHostAndPort() != null) {
            return Collections.singleton(redis.getHostAndPort());
        } else if (redis.getNodes() != null && !redis.getNodes().isEmpty()) {
            for (HostAndPort node : redis.getNodes()) {
                try (RedisProbe probe = new RedisProbe(node)) {
                    Cluster cluster = probe.discover();
                    Set<HostAndPort> nodes = new HashSet<>();
                    for (Node n : cluster.getNodes()) {
                        nodes.add(n.getRedis());
                    }
                    return nodes;
                } catch (IOException e) {
                    logger.warn("Failed to discover cluster info from node: {}", node, e);
                }
            }
        }
        return Collections.emptySet();
    }

}
