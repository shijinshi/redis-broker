package cn.shijinshi.redis.forward.server;

import cn.shijinshi.redis.common.Shutdown;
import cn.shijinshi.redis.forward.Handler;
import cn.shijinshi.redis.common.protocol.RedisRequest;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 将来自客户端的请求报文，交由Handlers进行处理，
 * 并将结果返回给客户端。
 *
 * @author Gui Jiahai
 */
@ChannelHandler.Sharable
public class RequestDispatcher extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(RequestDispatcher.class);

    private static final Tuple END = new Tuple(null, null);

    private final Handler handler;

    private final BlockingQueue<Tuple> queue = new LinkedBlockingQueue<>(1024 * 10);
    private final AtomicBoolean started = new AtomicBoolean(false);


    public RequestDispatcher(Handler handler) {
        this.handler = Objects.requireNonNull(handler);
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            Thread thread = new Thread(this::consume, "request-dispatcher-thread");
            thread.setDaemon(true);
            thread.start();
            Shutdown.addThread(thread);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

        RedisRequest request = (RedisRequest) msg;

        CompletableFuture<byte[]> future = new CompletableFuture<>();

        handler.handle(request, null, future);

        try {
            queue.put(new Tuple(future, ctx));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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

    private void consume() {
        while (started.get()) {
            Tuple tuple;
            try {
                tuple = queue.take();
            } catch (InterruptedException e) {
                logger.warn("{} was interrupted, left tuple will be processed, and end current thread", Thread.currentThread());
                break;
            }
            handleTuple(tuple);
        }

        logger.info("Detected closed is true, so queue.poll() instead of queue.take()");

        Tuple tuple;
        while ((tuple = queue.poll()) != null) {
            handleTuple(tuple);
        }

        try {
            handler.close();
        } catch (IOException ignored) {
        }

        logger.info("All request tuples was consumed");
    }

    private void handleTuple(Tuple tuple) {
        if (tuple == END) {
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
        if (started.compareAndSet(true, false)) {
            try {
                queue.put(END);
            } catch (InterruptedException ignored) {
            }
        } else {
            try {
                handler.close();
            } catch (IOException ignored) {
            }
        }
    }

    static class Tuple {
        final CompletableFuture<byte[]> future;
        final ChannelHandlerContext context;

        Tuple(CompletableFuture<byte[]> future, ChannelHandlerContext context) {
            this.future = future;
            this.context = context;
        }

        public CompletableFuture<byte[]> getFuture() {
            return future;
        }

        public ChannelHandlerContext getContext() {
            return context;
        }
    }

}
