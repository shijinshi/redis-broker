package cn.shijinshi.redis.forward.server;

import cn.shijinshi.redis.common.protocol.RedisRequest;
import cn.shijinshi.redis.forward.Handler;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * 将来自客户端的请求报文，交由Handlers进行处理，
 * 并将结果返回给客户端。
 *
 * @author Gui Jiahai
 */
@ChannelHandler.Sharable
public class RequestDispatcher extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(RequestDispatcher.class);

    private final Map<ChannelId, Channel> channels = new ConcurrentHashMap<>();
    private final Handler handler;
    private final Transfer transfer;

    public RequestDispatcher(Handler handler) {
        this.handler = Objects.requireNonNull(handler);
        transfer = createTransfer();
    }

    private Transfer createTransfer() {
        return new MultiQueuedTransfer(this::handleTuple);
    }

    public Map<ChannelId, Channel> getChannels() {
        return channels;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        RedisRequest request = (RedisRequest) msg;
        CompletableFuture<byte[]> future = new CompletableFuture<>();

        /*
        先将tuple入队，然后交由handler处理，
        两者顺序不能变。

        CompletableFuture future;
        future.thenAccept(consumer1);
        future.thenAccept(consumer2);

        当future完成后，会先执行consumer2，后执行consumer1。

        在handler处理request时，有可能会会在future.thenAccept(consumer)中，修改future中的result。
        在transfer.queue(tuple)中，会通过future.thenAccept(consumer)方法，
        取得结果，并将结果返回给client。

        综上，两者顺序不得变更。

        这确实是一个坏的设计，以后可能会优化。
         */

        transfer.queue(Tuple.create(ctx, future));
        handler.handle(request, null, future);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        channels.put(ctx.channel().id(), ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        channels.remove(ctx.channel().id());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        //每当client关闭socket连接时，就会报IOException
        if (cause instanceof IOException) {
            if (cause.getStackTrace() != null && cause.getStackTrace().length > 1) {
                StackTraceElement element = cause.getStackTrace()[1];
                if ("sun.nio.ch.SocketDispatcher".equals(element.getClassName()) && "read".equals(element.getMethodName())) {
                    return;
                }
            }
        }

        /*
          在处理request时，handler应该把异常通过future传递。
          如果发生异常，则关闭channel
         */
        logger.warn("Exception occurred, the channel will be closed, channel {}", ctx.channel(), cause);

        if (ctx.channel().isActive()) {
            ctx.close();
        }
    }

    private void handleTuple(Tuple tuple) {
        if (tuple == null) {
            try {
                handler.close();
            } catch (IOException ignored) {
            }
            return;
        }

        try {
            byte[] bytes = tuple.getFuture().get();
            if (tuple.getContext().channel().isActive()) {
                tuple.getContext().writeAndFlush(Unpooled.wrappedBuffer(bytes));
            }
        } catch (InterruptedException e) {
            logger.error("{} should not be interrupted", Thread.currentThread());
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            // 正常情况下，这里不应该出现异常。
            logger.error("There should be NO java.util.concurrent.ExecutionException. For security, we would close the channel[{}]",
                    tuple.getContext().channel().remoteAddress(), e.getCause());
            if (tuple.getContext().channel().isActive()) {
                tuple.getContext().close();
            }
        }
    }

    public void close() {
        if (transfer != null) {
            transfer.closeGracefully();
        }
    }

}
