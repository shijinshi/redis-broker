package cn.shijinshi.redis.forward.server;

import cn.shijinshi.redis.common.Shutdown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * @author Gui Jiahai
 */
public class SingleQueuedTransfer extends Transfer {

    private static final Logger logger = LoggerFactory.getLogger(SingleQueuedTransfer.class);

    private final Tuple end = Tuple.create(null, null);
    private final Queue<Tuple> queue = new ConcurrentLinkedQueue<>();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock r = rwLock.readLock();
    private final Lock w = rwLock.writeLock();
    private volatile boolean closed = false;

    public SingleQueuedTransfer(Consumer<Tuple> callback) {
        super(callback);

        Thread thread = new Thread(this::handleResult, "single-queued-transfer-thread");
        thread.setDaemon(true);
        thread.start();

        Shutdown.addThread(thread);
    }

    @Override
    public void queue(Tuple tuple) {
        int size = queue.size();
        r.lock();
        try {
            if (closed) {
                //todo 不应该出现在这里。如果出现在这里，应该是个bug
                logger.error("Queue is closed. No more tuple should be queued. This must be a bug.");
                throw new IllegalStateException("Queue closed");
            }
            queue.offer(tuple);
        } finally {
            r.unlock();
        }

        if (size == 0) {
            signalNotEmpty();
        }
    }

    private void signalNotEmpty() {
        lock.lock();
        try {
            condition.signal();
        } finally {
            lock.unlock();
        }
    }

    private void handleResult() {
        lock.lock();
        try {
            for (;;) {
                while (queue.isEmpty()) {
                    try {
                        condition.await();
                    } catch (InterruptedException e) {
                        logger.error("There should be NO InterruptedException while you take a tuple from queue. It must be a bug.", e);
                    }
                }

                Tuple tuple = queue.poll();
                if (tuple == null) {
                    continue;
                }

                if (tuple != end) {
                    apply(tuple);
                } else {
                    apply(null);
                    break;
                }
            }

        } finally {
            lock.unlock();
        }

        logger.info("All request tuples was consumed");
    }

    @Override
    public void closeGracefully() {
        w.lock();
        try {
            closed = true;
            queue.offer(end);
        } finally {
            w.unlock();
        }
    }
}
