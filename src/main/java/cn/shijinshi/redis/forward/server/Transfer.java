package cn.shijinshi.redis.forward.server;

import org.slf4j.Logger;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Redis协议多请求可以复用单个连接，在同一个网络连接中，
 * 要求回复报文的顺序必须要与请求报文的顺序一致。
 *
 * 在RequestDispatcher，将接收到的报文交由Handler异步进行处理。
 * 但是，最终如何将结果按照顺序返回给client呢？
 *
 * 思路是，将请求和对应的future，放入队列中，
 * 在另一个线程中，从这个队列中，逐个取出请求和对应的future，
 * 然后，通过future.get()方法，逐个获取结果，并返回给client。
 *
 * 有两种实现，
 * SingleQueuedTransfer, MultiQueuedTransfer
 *
 * 目前推荐使用SingleQueuedTransfer。
 * MultiQueuedTransfer还有些问题没想清楚，而且性能不一定
 * 比SingleQueuedTransfer好多少。
 *
 * @author Gui Jiahai
 */
public abstract class Transfer {

    private final Consumer<Tuple> callback;
    protected volatile boolean closed = false;

    protected Transfer(Consumer<Tuple> callback) {
        this.callback = Objects.requireNonNull(callback);
    }

    /**
     * Transfer的队列中，每当tuple.future处于完成状态后，
     * Transfer会将tuple出队，这时候，
     * Transfer的实现类应该调用此方法，交由caller去处理tuple。
     *
     * @param tuple 当tuple为null时，表示Transfer已经被closed了，
     *              而且，内部队列中的所有tuple都已经被处理了。
     *              caller可以在这时候处理一些清理操作。
     */
    protected void apply(Tuple tuple) {
        callback.accept(tuple);
    }

    /**
     * 检查当前状态 - closed
     * 如果是closed = true，抛出异常
     */
    protected void checkClosed(Logger logger) throws IllegalStateException {
        if (closed) {
            //todo 不应该出现在这里。如果出现在这里，应该是个bug
            logger.error("Transfer closed. No more tuple should be queued. This must be a bug.");
            throw new IllegalStateException("Transfer closed");
        }
    }

    /**
     * 调用此方法，将tuple入队。
     *
     * @throws IllegalStateException 当调用closeGracefully()后，如果
     * 再执行此方法，则会抛出异常。
     */
    public abstract void queue(Tuple tuple);

    /**
     * 用于程序的优雅关机。
     * 调用此方法，使Transfer处于closed状态，
     * 那么Transfer在处理完队列中的tuple后，会自动关闭，
     * 而且，调用apple(null)，通知caller。
     *
     * 在closed状态后，caller不应该再调用queue(Tuple)，
     * 否则会抛出异常。
     */
    public abstract void closeGracefully();

}
