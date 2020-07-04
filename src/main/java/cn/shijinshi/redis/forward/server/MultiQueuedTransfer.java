package cn.shijinshi.redis.forward.server;

import cn.shijinshi.redis.common.Shutdown;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.FastThreadLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * 采用多个队列来维护tuple。
 *
 * 在 SingleQueuedTransfer 中，采用单个队列，
 * 那么在多个线程同时执行queue()方法时，虽然不会造成
 * 线程堵塞，但是也会多次执行CAS操作，降低效率。
 *
 * 在Transfer中说过，要求在同一个网络连接中，tuple有序。
 * 那么，可以为每个线程创建一个Queue，这样并不会造成
 * 多线程同时操作一个Queue。
 *
 * @author Gui Jiahai
 */
public class MultiQueuedTransfer extends Transfer {

    private static final Logger logger = LoggerFactory.getLogger(MultiQueuedTransfer.class);

    private final AtomicInteger nExecutors = new AtomicInteger(0);
    private final CountDownLatch waiter = new CountDownLatch(1);
    private final List<CompletableFuture<Void>> closeFutures = Collections.synchronizedList(new ArrayList<>());

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock r = rwLock.readLock();
    private final Lock w = rwLock.writeLock();

    private final FastThreadLocal<Queue<Tuple>> threadQueueMap = new FastThreadLocal<>();
    private final FastThreadLocal<Boolean> threadStateMap = new FastThreadLocal<>();

    public MultiQueuedTransfer(Consumer<Tuple> callback) {
        super(callback);

        Shutdown.addRunner(() -> {
            try {
                waiter.wait();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }, Integer.MAX_VALUE - 100);
    }

    private Queue<Tuple> getQueue(EventExecutor executor) {
        Queue<Tuple> queue = threadQueueMap.get();
        if (queue == null) {
            CompletableFuture<Void> f = new CompletableFuture<>();
            f.thenRun(() -> executor.execute(MultiQueuedTransfer.this::handleResult));
            closeFutures.add(f);

            //记录Executor的个数，以便于后期释放
            nExecutors.incrementAndGet();

            /*
            这里的队列是用数组呢，还是链表呢？
            理论上，数组性能更好一点，还能减少GC。
            但是，数组有可能在压力大的时候，会变得很大，
            压力小的时候，却不能释放多余的空间。
            而链表却能很好的释放内存。
             */
            threadQueueMap.set(queue = new ArrayDeque<>());
        }
        return queue;
    }

    @Override
    public void queue(Tuple tuple) {
        EventExecutor executor = tuple.getContext().executor();
        r.lock();
        try {
            checkClosed(logger);
            Queue<Tuple> queue = getQueue(executor);
            queue.offer(tuple);
        } finally {
            r.unlock();
        }

        tuple.getFuture().thenRun(() -> {
            if (executor.inEventLoop()) {
                handleResult();
            } else {
                executor.execute(this::handleResult);
            }
        });
    }

    private void handleResult() {
        Queue<Tuple> queue = threadQueueMap.get();
        Tuple tuple;
        while ((tuple = queue.peek()) != null) {
            if (tuple.getFuture().isDone()) {
                apply(queue.poll());
            } else {
                break;
            }
        }

        Boolean state;
        if (closed && queue.isEmpty()
                && ((state = threadStateMap.get()) == null || !state)) {
            threadStateMap.set(Boolean.TRUE);
            if (nExecutors.decrementAndGet() == 0) {
                apply(null);
                waiter.countDown();
            }
        }
    }

    @Override
    public void closeGracefully() {
        w.lock();
        try {
            closed = true;
        } finally {
            w.unlock();
        }

        for (CompletableFuture<Void> future : closeFutures) {
            future.complete(null);
        }
    }
}
