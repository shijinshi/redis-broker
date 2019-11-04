package cn.shijinshi.redis.forward.client;

import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.common.protocol.RedisRequest;
import cn.shijinshi.redis.common.util.UnsafeByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

/**
 * @author Gui Jiahai
 */
public class RedisConnectorTest {

    private AutoRedisConnector connector;

    @Before
    public void setup() {
        connector = new AutoRedisConnector(HostAndPort.create("192.168.100.101", 6179));
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
    }

    @Test
    public void test_request() throws InterruptedException, ExecutionException, TimeoutException {
        ByteBuf pingRequest = Unpooled.wrappedBuffer("*1\r\n$4\r\nPING\r\n".getBytes());
        String random = UUID.randomUUID().toString().replaceAll("-", "");
        String setRequest = "*4\r\n$5\r\nSETEX\r\n$" + random.length() + "\r\n" + random + "\r\n$2\r\n60\r\n$" + random.length() + "\r\n" + random + "\r\n";

        String getRequestStr = "*2\r\n$3\r\nGET\r\n$" + random.length() + "\r\n" + random + "\r\n";
        RedisRequest getRequest = new RedisRequest(getRequestStr.getBytes(), new UnsafeByteString("GET"), null);


        CompletableFuture<byte[]> future;
        byte[] bytes;

        future = new CompletableFuture<>();
        connector.send(pingRequest, future);
        bytes = future.get(3, TimeUnit.SECONDS);
        Assert.assertArrayEquals(bytes, "+PONG\r\n".getBytes());

        future = new CompletableFuture<>();
        connector.send(setRequest.getBytes(), future);
        bytes = future.get(3, TimeUnit.SECONDS);
        Assert.assertArrayEquals(bytes, "+OK\r\n".getBytes());

        future = new CompletableFuture<>();
        connector.send(getRequest, future);
        bytes = future.get(3, TimeUnit.SECONDS);
        Assert.assertArrayEquals(bytes, ("$" + random.length() + "\r\n" + random + "\r\n").getBytes());
    }


    @After
    public void after() {
        if (connector != null) {
            connector.close();
        }
    }

}
