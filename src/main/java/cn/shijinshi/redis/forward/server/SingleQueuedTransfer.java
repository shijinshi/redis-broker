package cn.shijinshi.redis.forward.server;

import cn.shijinshi.redis.common.Shutdown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.*;
import java.util.function.Consumer;

/**
 * 采用单个队列来维护tuple。
 *
 * 另外，创建一个线程，此线程不断地从队列中消费tuple，
 * 待tuple.future处于完成状态后，将调用Transfer.apply方法，
 * 处理tuple，然后，继续消费队列中的tuple。
 *
 * 起初，此类的队列采用了BlockingQueue，但是，在入队的时候，
 * 由于竞争，会产生线程的上下文切换，从而效率低。
 *
 * 最后，使用了ConcurrentLinkedQueue - 非阻塞线程安全的队列。
 * 好处是，在入队时，不会使线程堵塞。
 *
 * @author Gui Jiahai
 */
@Deprecated
public class SingleQueuedTransfer extends Transfer {

    private static final Logger logger = LoggerFactory.getLogger(SingleQueuedTransfer.class);

    private final Tuple end = Tuple.create(null, null);
    private final Queue<Tuple> queue = new ConcurrentLinkedQueue<>();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock r = rwLock.readLock();
    private final Lock w = rwLock.writeLock();

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

        /*
        加写锁，判断closed状态，
         */
        r.lock();
        try {
            checkClosed(logger);
            queue.offer(tuple);
        } finally {
            r.unlock();
        }

        /*
        如果size == 0，则Thread有可能处于wait状态，
        这时候，应该发出信号。
         */
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
        for (;;) {
            lock.lock();
            try {
                while (queue.isEmpty()) {
                    try {
                        condition.await();
                    } catch (InterruptedException e) {
                        logger.error("There should be NO InterruptedException while you take a tuple from queue. It must be a bug.", e);
                    }
                }
            }finally {
                lock.unlock();
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
