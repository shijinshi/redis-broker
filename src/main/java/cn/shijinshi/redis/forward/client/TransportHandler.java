package cn.shijinshi.redis.forward.client;

import cn.shijinshi.redis.common.Constants;
import cn.shijinshi.redis.common.protocol.RedisRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;

/**
 * 处理解析后的请求报文
 *
 * @author Gui Jiahai
 */
public class TransportHandler extends ChannelDuplexHandler {

    private static final Logger logger = LoggerFactory.getLogger(TransportHandler.class);

    private final Deque<CompletableFuture<byte[]>> deque = new ArrayDeque<>();

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof RequestAndFuture) {
            RequestAndFuture rf = (RequestAndFuture) msg;
            Object r = rf.getRequest();

            if (promise.isVoid()) {
                deque.add(rf.getFuture());
            } else {
                promise.addListener(future -> {
                    if (future.isSuccess()) {
                        deque.add(rf.getFuture());
                    } else {
                        rf.getFuture().complete(Constants.CONNECTION_LOST);
                    }
                });
            }

            if (r instanceof byte[]) {
                ctx.write(Unpooled.wrappedBuffer((byte[]) r), promise);
            } else if (r instanceof RedisRequest) {
                ctx.write(Unpooled.wrappedBuffer(((RedisRequest) r).getContent()), promise);
            } else if (r instanceof ByteBuf) {
                ctx.write(r, promise);
            } else {
                throw new Error("Cannot handle request of type: " + r.getClass());
            }

        } else {
            super.write(ctx, msg, promise);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof byte[]) {
            byte[] bytes = (byte[]) msg;
            CompletableFuture<byte[]> future = deque.poll();
            if (future != null) {
                future.complete(bytes);
            } else {
                logger.warn("Cannot poll future from deque, may be error");
            }
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        if (!deque.isEmpty()) {
            CompletableFuture<byte[]> future;
            while ((future = deque.poll()) != null) {
                future.complete(Constants.CONNECTION_LOST);
            }
        }
    }
}
