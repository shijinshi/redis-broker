package cn.shijinshi.redis.forward.server;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Redis协议多请求可以复用单个连接，由于报文并不携带id，
 * 所以，回复报文的顺序必须要与请求报文的顺序一致。
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

    protected Transfer(Consumer<Tuple> callback) {
        this.callback = Objects.requireNonNull(callback);
    }

    protected void apply(Tuple tuple) {
        callback.accept(tuple);
    }

    public abstract void queue(Tuple tuple);

    public abstract void closeGracefully();

}
