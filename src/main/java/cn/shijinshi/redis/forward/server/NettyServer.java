package cn.shijinshi.redis.forward.server;

import cn.shijinshi.redis.common.Constants;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Supplier;

/**
 * @author Gui Jiahai
 */
public class NettyServer {

    private static final Logger logger = LoggerFactory.getLogger(NettyServer.class);

    private final int port;

    private final ServerBootstrap bootstrap;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private Channel channel;

    public NettyServer(int port, Supplier<List<ChannelHandler>> handlers) {
        this.port = port;
        logger.info("Starting server: {}", this.port);

        this.bootstrap = new ServerBootstrap();

        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(Constants.DEFAULT_IO_THREADS);

        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, Boolean.TRUE)
                .childOption(ChannelOption.SO_REUSEADDR, Boolean.TRUE)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childHandler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) {
                        for (ChannelHandler handler : handlers.get()) {
                            ch.pipeline().addLast(handler);
                        }
                    }
                });
    }

    public void open() {
        ChannelFuture channelFuture = bootstrap.bind(this.port).syncUninterruptibly();
        if (!channelFuture.isSuccess()) {
            logger.error("NettyServer cannot bind port: {}", this.port, channelFuture.cause());
            throw new IllegalStateException("NettyServer cannot bind port: " + this.port, channelFuture.cause());
        }
        this.channel = channelFuture.channel();
    }

    public void close() {

        logger.info("Closing netty server: {}", this.port);

        if (channel != null) {
            channel.close();
            channel = null;
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }

}
