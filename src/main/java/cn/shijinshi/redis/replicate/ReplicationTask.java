package cn.shijinshi.redis.replicate;

import cn.shijinshi.redis.common.adapt.ReplicatorAdaptor;
import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.common.util.ArrayUtil;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisReplicator;
import com.moilioncircle.redis.replicator.cmd.impl.SetExCommand;
import com.moilioncircle.redis.replicator.event.PostRdbSyncEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.*;

/**
 * 复制任务
 * 将单个Redis节点的数据复制到target redis (cluster) 中。
 *
 * @author Gui Jiahai
 */
public class ReplicationTask extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationTask.class);

    private static final Future<Void> end = CompletableFuture.completedFuture(null);

    private final BlockingQueue<Future<Void>> queue = new LinkedBlockingDeque<>(1024 * 10);

    private final HostAndPort source;
    private final RedisReplicator replicator;
    private final Observer<HostAndPort> observer;

    private volatile boolean error = false;
    private volatile byte[] tick = null;


    public ReplicationTask(HostAndPort source, ReplicatorAdaptor adaptor, Observer<HostAndPort> observer) {
        super(String.format("replication-task-%s-thread", source.format()));
        setDaemon(true);

        this.source = Objects.requireNonNull(source);
        this.observer = observer;

        Configuration configuration = Configuration.defaultSetting();
        configuration.setRetries(1);
        replicator = new RedisReplicator(source.getHost(), source.getPort(), configuration);

        replicator.addEventListener((replicator, event) -> {
            if (event instanceof SetExCommand && tick != null) {
                if (ArrayUtil.startsWith(((SetExCommand) event).getKey(), tick)) {
                    try {
                        this.replicator.close();
                    } catch (IOException ignored) {}
                    return;
                }
            } else if (event instanceof PostRdbSyncEvent) {
                Observer<HostAndPort> obs;
                if ((obs = this.observer) != null) {
                    obs.onNext(this.source);
                }
                return;
            }

            try {
                queue.put(adaptor.dispatch(event));
            } catch (InterruptedException ignored) {
                logger.info("{} {} should not be interrupted", logPrefix(), Thread.currentThread());
            }
        });
        replicator.addExceptionListener((replicator, throwable, event) -> {
            logger.error("{} replicator occurred error", logPrefix(), throwable);
            error = true;
            try {
                this.replicator.close();
            } catch (IOException ignored) {}
        });

        logger.info("{} created replicate task", logPrefix());
    }

    private String logPrefix() {
        return "source[" + source.format() + "]";
    }

    @Override
    public void run() {

        long start = System.nanoTime();

        Thread thread = new Thread(() -> {
            try {
                handleFuture();
            } catch (InterruptedException e) {
                logger.info("{} {} should not be interrupted", logPrefix(), Thread.currentThread());
            }
        }, String.format("replication-task-future-%s-thread", source.format()));
        thread.setDaemon(true);
        thread.start();

        try {
            replicator.open();
        } catch (Throwable t) {
            error = true;
            logger.error("{} replicator occurred error", logPrefix(), t);
        } finally {
            try {
                queue.put(end);
                thread.join();
            } catch (InterruptedException e) {
                logger.error("{} {} should not be interrupted", logPrefix(), Thread.currentThread());
            }

            long cost = TimeUnit.NANOSECONDS.toMillis((System.nanoTime() - start));
            if (error) {
                logger.error(">>>>> {} replication failed, cost time: {}ms", logPrefix(), cost);
            } else {
                logger.info(">>>>> {} replication succeeded, cost time: {}ms", logPrefix(), cost);
            }

            Observer<HostAndPort> obs;
            if ((obs = this.observer) != null) {
                if (error) {
                    obs.onError(this.source);
                } else {
                    obs.onCompleted(this.source);
                }
            }
        }
    }

    private void handleFuture() throws InterruptedException {
        Future<Void> future;
        while ((future = queue.take()) != end) {
            try {
                future.get();
            } catch (ExecutionException e) {
                logger.error("Failed to get reply from future", e);
                error = false;
                try {
                    this.replicator.close();
                } catch (IOException ignored) {}
            }
        }
    }

    public void completeWithTick(byte[] tick) {
        if (tick == null || tick.length == 0) {
            throw new IllegalArgumentException("tick must not be null or empty");
        }

        this.tick = tick;
        logger.info("{} ready to complete with tick", logPrefix());
    }

    @Override
    public void interrupt() {
        logger.error("{} replication was cancelled", logPrefix());
        error = true;
        try {
            this.replicator.close();
        } catch (IOException ignored) {}
    }

}
