package cn.shijinshi.redis.forward.client;

import cn.shijinshi.redis.common.Shutdown;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * 带有重连功能的client
 *
 * @author Gui Jiahai
 */
public class NettyClient {

    private static final Logger logger = LoggerFactory.getLogger(NettyClient.class);

    private static final NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup();

    static {
        Shutdown.addRunnable(eventLoopGroup::shutdownGracefully);
    }

    private static final long DELAY_UPPER_BOUND = 1000;

    private final Bootstrap bootstrap;
    private final InetSocketAddress address;
    private volatile Channel channel;

    private final Lock connectLock = new ReentrantLock();
    private int attempts = 0;
    private boolean closed = false;

    public NettyClient(InetSocketAddress address, Supplier<List<ChannelHandler>> handlers) {
        this.address = Objects.requireNonNull(address);
        bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup)
                .option(ChannelOption.SO_KEEPALIVE, Boolean.TRUE)
                .option(ChannelOption.TCP_NODELAY, Boolean.TRUE)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) {
                        for (ChannelHandler handler : handlers.get()) {
                            ch.pipeline().addLast(handler);
                        }
                        ch.pipeline().addLast(new Reconnection());
                    }
                });
    }

    public void connect(boolean async) {
        doConnect(async);
    }

    private void doConnect(boolean async) {
        if (closed) {
            return;
        }
        ChannelFuture channelFuture = bootstrap.connect(address);
        if (async) {
            ChannelFuture finalChannelFuture = channelFuture;
            channelFuture.addListener(future -> doConnect0(finalChannelFuture, future));

        } else {
            try {
                channelFuture = channelFuture.syncUninterruptibly();
                doConnect0(channelFuture, channelFuture);
            } catch (Throwable t) {
                logger.error("Unable to connect target[{}], will be reconnected", address, t);
            }

        }
    }

    private void doConnect0(ChannelFuture finalChannelFuture, Future<? super Void> future) {
        connectLock.lock();
        try {
            if (future.isSuccess()) {
                if (closed) {
                    finalChannelFuture.channel().close();
                } else {
                    logger.info("Connected to redis server[{}]", address);
                    attempts = 0;
                    this.channel = finalChannelFuture.channel();
                }
            } else {
                logger.info("Failed to client redis server[{}]", address);
                reconnect();
            }
        } finally {
            connectLock.unlock();
        }
    }

    private void reconnect() {
        if (closed) {
            return;
        }
        long delay = delay();
        logger.info("reconnect to server[{}] after {} ms", this.address, delay);
        if (delay == 0) {
            eventLoopGroup.execute(() -> this.doConnect(true));
        } else {
            eventLoopGroup.schedule(() -> this.doConnect(true), delay, TimeUnit.MILLISECONDS);
        }
        attempts ++;
    }

    private long delay() {
        return Math.min(attempts * 1000, DELAY_UPPER_BOUND);
    }

    /**
     * 在调用此方法之前，应该通过 isConnected() 来判断当前连接状态。
     * 如果当前处于未连接状态，则不应该调用此方法
     */
    public void send(Object msg) {
        channel.writeAndFlush(msg);
    }

    public boolean isConnected() {
        Channel c = channel;
        return c != null && c.isActive();
    }

    public void close() {
        connectLock.lock();
        try {
            if (!closed) {
                closed = true;
                Channel c = this.channel;
                this.channel = null;
                if (c != null) {
                    c.close();
                }
            }
        } finally {
            connectLock.unlock();
        }
    }

    class Reconnection extends ChannelInboundHandlerAdapter {
        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            super.channelInactive(ctx);
            reconnect();
        }
    }

}
