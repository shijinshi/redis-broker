package cn.shijinshi.redis.forward.client;

import cn.shijinshi.redis.common.Constants;
import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.control.broker.NodeListener;
import cn.shijinshi.redis.forward.RedisConnector;
import io.netty.channel.ChannelHandler;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Redis连接器。
 * 另外，此类实现了{@link NodeListener} 接口，
 * 所以，可以动态地切换Redis节点。
 *
 * @author Gui Jiahai
 */
public class AutoRedisConnector implements RedisConnector, NodeListener, Supplier<List<ChannelHandler>> {

    private static final byte[] CONNECTION_CLOSED = "-ERR connection closed\r\n".getBytes();

    private volatile NettyClient nettyClient;
    private volatile HostAndPort address;

    private volatile boolean closed = false;

    public AutoRedisConnector() {
    }

    public AutoRedisConnector(HostAndPort address) {
        this.address = Objects.requireNonNull(address);
        connect0(false);
    }

    @Override
    public void send(Object request, CompletableFuture<byte[]> future) {
        if (closed) {
            future.complete(CONNECTION_CLOSED);
            return;
        }
        NettyClient client = this.nettyClient;
        if (client != null && client.isConnected()) {
            client.send(new RequestAndFuture(request, future));
        } else {
            future.complete(Constants.CONNECTION_LOST);
        }
    }

    @Override
    public synchronized void nodeChanged(HostAndPort address) {
        if (closed) {
            return;
        }

        if (address == null) {
            close0();
        } else {
            if (!address.equals(this.address)) {
                this.address = address;
                close0();
                connect0(true);
            }
        }
    }

    private void connect0(boolean async) {
        InetSocketAddress socketAddress = new InetSocketAddress(address.getHost(), address.getPort());
        NettyClient newClient = new NettyClient(socketAddress, this);
        newClient.connect(async);
        this.nettyClient = newClient;
    }

    private void close0() {
        NettyClient oldClient = this.nettyClient;
        if (oldClient != null) {
            this.nettyClient = null;
            oldClient.close();
        }
    }

    @Override
    public List<ChannelHandler> get() {
        return Arrays.asList(new ReplyDecoder(), new TransportHandler());
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        NettyClient client = this.nettyClient;
        this.nettyClient = null;

        if (client != null) {
            client.close();
        }
    }
}
