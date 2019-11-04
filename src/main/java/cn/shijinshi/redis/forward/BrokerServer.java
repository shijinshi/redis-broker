package cn.shijinshi.redis.forward;

import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.forward.server.NettyServer;
import cn.shijinshi.redis.forward.server.RequestDecoder;
import cn.shijinshi.redis.forward.server.RequestDispatcher;
import io.netty.channel.ChannelHandler;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * @author Gui Jiahai
 */
public class BrokerServer implements Supplier<List<ChannelHandler>> {

    private final BrokerProperties properties;
    private final Handler handler;

    private RequestDispatcher requestDispatcher;
    private NettyServer nettyServer;

    public BrokerServer(BrokerProperties properties, Handler handler) {
        this.properties = Objects.requireNonNull(properties);
        this.handler = Objects.requireNonNull(handler);
    }

    public void start() {
        try {
            this.requestDispatcher = new RequestDispatcher(handler);
            this.nettyServer = new NettyServer(this.properties.getPort(), this);
            this.nettyServer.open();
            this.requestDispatcher.start();
        } catch (Throwable t) {
            stop();
            throw t;
        }
    }

    public void stop() {
        if (this.nettyServer != null) {
            this.nettyServer.close();
        }
        if (this.requestDispatcher != null) {
            this.requestDispatcher.close();
        }
    }

    @Override
    public List<ChannelHandler> get() {
        return Arrays.asList(new RequestDecoder(), requestDispatcher);
    }
}
