package cn.shijinshi.redis.sync;

import cn.shijinshi.redis.common.Shutdown;
import cn.shijinshi.redis.common.adapt.AccessibleException;
import cn.shijinshi.redis.common.adapt.ReplicatorAdaptor;
import cn.shijinshi.redis.common.adapt.ReplicatorAdaptorFactory;
import cn.shijinshi.redis.common.error.ErrorHandler;
import cn.shijinshi.redis.common.log.IndexLogger;
import cn.shijinshi.redis.common.prop.RedisProperties;
import cn.shijinshi.redis.common.util.ArrayUtil;
import com.moilioncircle.redis.replicator.Configuration;
import com.moilioncircle.redis.replicator.RedisAofReplicator;
import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.Status;
import com.moilioncircle.redis.replicator.cmd.impl.SetCommand;
import com.moilioncircle.redis.replicator.cmd.impl.SetExCommand;
import com.moilioncircle.redis.replicator.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 设计思想：
 * 此类用于将磁盘队列中的request发送到对端redis数据库中。
 * 如果进程退出时，有可能会使磁盘队列中的request没有全部发送对端redis数据库中，
 * 从而造成数据不一致。所以，强烈要求，在进程退出时，使用非守护线程，
 * 将队列中的request发送到对端redis数据库中，直到队列中没有request。
 */
public class RequestSynchronizer extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(RequestSynchronizer.class);

    private final IndexLogger indexLogger;
    private final ReplicatorAdaptor adaptor;
    private final Replicator replicator;

    private final BlockingQueue<Tuple> queue = new LinkedBlockingQueue<>(256);
    private final AtomicBoolean started = new AtomicBoolean(false);

    private int requestSize = 0;
    private volatile boolean paused = false;
    private volatile byte[] tick = null;

    private final CountDownLatch stopLatch = new CountDownLatch(1);

    public RequestSynchronizer(IndexLogger indexLogger, RedisProperties target) {
        super("request-synchronizer-future-thread");

        this.indexLogger = Objects.requireNonNull(indexLogger);
        this.adaptor = ReplicatorAdaptorFactory.create(target);

        InputStream inputStream = indexLogger.getInputStream();
        replicator = new RedisAofReplicator(inputStream, Configuration.defaultSetting());
        replicator.addEventListener((replicator, event) -> {
            if (paused && started.get()) {
                logger.info(">>> Enter pause status");
                long start = System.nanoTime();
                while (paused && started.get()) {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                long cost = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                logger.info(">>> Exit pause status, cost: {} ms", cost);
            }

            if (tick != null) {
                discard(event);
                return;
            }

            int size = requestSize;
            requestSize = 0;

            Future<Void> future = adaptor.dispatch(event);
            Tuple tuple = new Tuple(event, size, future);
            try {
                queue.put(tuple);
            } catch (InterruptedException ignored) {
                logger.error("{} should not be interrupted", Thread.currentThread());
            }
        });
        replicator.addRawByteListener(len -> requestSize += len);
        replicator.addExceptionListener((replicator, throwable, event)
                -> logger.error("Replicator error: {}", event.getClass(), throwable));
    }

    @Override
    public synchronized void start() {
        super.start();
        Shutdown.addThread(this);
    }

    private void discard(Event event) {
        byte[] key = null;
        if (event instanceof SetExCommand) {
            key = ((SetExCommand) event).getKey();
        } else if (event instanceof SetCommand) {
            key = ((SetCommand) event).getKey();
        }
        if (key != null && ArrayUtil.startsWith(key, tick)) {
            tick = null;
            logger.info(">>> End discard and the request synchronizer run normally");
        }
    }

    @Override
    public void run() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        Thread t1 = new Thread(() -> {
            try {
                replicator.open();
            } catch (Throwable t) {
                logger.error("Replicator error, and synchronizer will be interrupted, please solve this problem and restart", t);
            } finally {
                try {
                    queue.put(Tuple.END);
                } catch (InterruptedException ignored) {
                    logger.error("{} should not be interrupted", Thread.currentThread());
                } finally {
                    stopLatch.countDown();
                }
            }
        }, "request-synchronizer-replicator-thread");
        t1.setDaemon(true);
        t1.start();

        try {
            handleFuture();
        } catch (InterruptedException e) {
            logger.error("{} should not be interrupted", Thread.currentThread());
            ErrorHandler.handle(e);
        } finally {
            try {
                indexLogger.getInputStream().close();
            } catch (IOException ignored) {}
        }
    }

    private void handleFuture() throws InterruptedException {
        Tuple tuple;
        while ((tuple = queue.take()) != Tuple.END) {
            try {
                tuple.getFuture().get();
                indexLogger.ack(tuple.getSize());
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof AccessibleException) {
                    logger.error("Failed to get reply, event will be retried", cause);

                    if (!retry(tuple, (AccessibleException) cause)) {
                        logger.error("Failed to retry event, we will give up and exit current thread");
                        return;
                    }

                } else {
                    logger.error("Failed to get reply, the event will be ignored", cause);
                    indexLogger.ack(tuple.getSize());
                }
            }
        }
    }

    private boolean retry(Tuple tuple, AccessibleException ex) throws InterruptedException {
        while (!(replicator.getStatus() == Status.DISCONNECTED && ex.isDisconnected())) {
            tuple.incrAttempts();
            long delay;
            if ((delay = delay(tuple.getAttempts())) > 0) {
                Thread.sleep(delay);
            }
            Future<Void> future = adaptor.dispatch(tuple.getEvent());
            try {
                future.get();
                indexLogger.ack(tuple.getSize());
                return true;
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof AccessibleException) {
                    ex = (AccessibleException) cause;
                    logger.info("Failed to get reply in retry[{}], event will be retried", cause);
                } else {
                    logger.info("Failed to get reply in retry[{}], event will be ignored", cause);
                }
            }
        }
        return false;
    }

    private long delay(int attempts) {
        return Math.min((attempts - 1) * 1000, 3000);
    }

    public void pause() {
        logger.info(">>> Pause");
        paused = true;
        tick = null;
    }

    public void resume_() {
        logger.info(">>> Resume");
        paused = false;
        tick = null;
    }

    public void resumeTick(byte[] tick) {
        logger.info(">>> Resume with tick: {}", new String(tick));

        this.tick = tick;
        this.paused = false;
    }

    public boolean isPaused() {
        return paused;
    }

    public boolean isTicked() {
        return tick != null;
    }

    public void stop(long timeout, TimeUnit timeUnit) {
        if (started.compareAndSet(true, false)) {
            if (timeout <= 0) {
                doStop();
            } else {
                Thread thread = new Thread(() -> {
                    try {
                        stopLatch.await(timeout, timeUnit);
                    } catch (InterruptedException ignored) {
                    } finally {
                        doStop();
                    }
                }, "request-synchronizer-stop");
                thread.setDaemon(true);
                thread.start();
                Shutdown.addThread(thread);
            }
        }
    }

    private void doStop() {
        if (this.replicator != null) {
            try {
                this.replicator.close();
            } catch (IOException ignored) {}
        }
    }

    static class Tuple {

        static final Tuple END = new Tuple(null, 0, null);

        private final Event event;
        private final int size;
        private final Future<Void> future;
        private int attempts = 0;

        Tuple(Event event, int size, Future<Void> future) {
            this.event = event;
            this.size = size;
            this.future = future;
        }

        Event getEvent() {
            return event;
        }

        int getSize() {
            return size;
        }

        public Future<Void> getFuture() {
            return future;
        }

        int getAttempts() {
            return attempts;
        }

        void incrAttempts() {
            attempts ++;
        }
    }
}
