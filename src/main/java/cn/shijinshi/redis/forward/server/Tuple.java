package cn.shijinshi.redis.forward.server;

import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.CompletableFuture;

/**
 * @author Gui Jiahai
 */
public class Tuple {

    private final ChannelHandlerContext context;

    private final CompletableFuture<byte[]> future;

    public static Tuple create(ChannelHandlerContext context, CompletableFuture<byte[]> future) {
        return new Tuple(context, future);
    }

    public Tuple(ChannelHandlerContext context, CompletableFuture<byte[]> future) {
        this.context = context;
        this.future = future;
    }

    public ChannelHandlerContext getContext() {
        return context;
    }

    public CompletableFuture<byte[]> getFuture() {
        return future;
    }

}
