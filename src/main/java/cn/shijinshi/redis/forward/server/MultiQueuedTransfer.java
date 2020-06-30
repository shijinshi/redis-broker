package cn.shijinshi.redis.forward.server;

import cn.shijinshi.redis.common.Shutdown;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.FastThreadLocal;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

/**
 *
 *
 * @author Gui Jiahai
 */
@Deprecated
public class MultiQueuedTransfer extends Transfer {

    private final CountDownLatch waiter;

    private final FastThreadLocal<Queue<Tuple>> threadLocal = new FastThreadLocal<Queue<Tuple>>() {
        @Override
        protected Queue<Tuple> initialValue() {
            return new LinkedList<>();
        }
    };

    public MultiQueuedTransfer(Consumer<Tuple> callback, int nExecutors) {
        super(callback);
        this.waiter = new CountDownLatch(nExecutors);

        Shutdown.addRunner(waiter::countDown, Integer.MAX_VALUE - 100);
    }

    @Override
    public void queue(Tuple tuple) {
        Queue<Tuple> queue = threadLocal.get();
        queue.offer(tuple);

        tuple.getFuture().thenRun(() -> {
            EventExecutor executor = tuple.getContext().executor();
            if (!executor.inEventLoop()) {
                executor.execute(this::handleResult);
            } else {
                handleResult();
            }
        });
    }

    private void handleResult() {
        Queue<Tuple> queue = threadLocal.get();
        Tuple tuple;
        while ((tuple = queue.peek()) != null) {
            if (tuple.getFuture().isDone()) {
                apply(queue.remove());
            } else {
                break;
            }
        }
    }

    @Override
    public void closeGracefully() {

    }
}
