package cn.shijinshi.redis.forward.handler;

import cn.shijinshi.redis.common.param.Cluster;
import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.common.param.Node;
import cn.shijinshi.redis.common.param.Range;
import cn.shijinshi.redis.common.protocol.RedisRequest;
import cn.shijinshi.redis.control.broker.Broker;
import cn.shijinshi.redis.forward.RedisConnector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Gui Jiahai
 */
@RunWith(MockitoJUnitRunner.class)
public class ConnectionHandlerTest {

    @InjectMocks
    private ConnectionHandler handler;

    @Mock
    private RedisConnector connector;
    @Mock
    private Broker broker;

    private final RedisRequest request = new RedisRequest(null, null, null);

    @Before
    public void setup() {

        Node node1 = new Node();
        node1.setRedis(HostAndPort.create("192.168.100.101", 7001));
        node1.setBroker(HostAndPort.create("192.168.100.201", 7001));
        node1.setRanges(Collections.singletonList(new Range(0, 5000)));

        Node node2 = new Node();
        node2.setRedis(HostAndPort.create("192.168.100.101", 7002));
        node2.setBroker(HostAndPort.create("192.168.100.201", 7002));
        node2.setRanges(Collections.singletonList(new Range(5001, 10000)));

        Node node3 = new Node();
        node3.setRedis(HostAndPort.create("192.168.100.101", 7003));
        node3.setBroker(HostAndPort.create("192.168.100.201", 7003));
        node3.setRanges(Collections.singletonList(new Range(10001, 16383)));

        Cluster cluster = new Cluster(Arrays.asList(node1, node2, node3));
        when(broker.getCluster()).thenReturn(cluster);
    }

    @Test
    public void test_normal() throws ExecutionException, InterruptedException {
        CompletableFuture<byte[]> future;

        future = new CompletableFuture<>();
        handler.handle(request, null, future);
        future.complete("+OK\r\n".getBytes());
        Assert.assertArrayEquals(future.get(), "+OK\r\n".getBytes());

        future = new CompletableFuture<>();
        handler.handle(request, null, future);
        future.complete("-ERR BAD MSG\r\n".getBytes());
        Assert.assertArrayEquals(future.get(), "-ERR BAD MSG\r\n".getBytes());

        verify(connector).send(request.getContent(), future);
    }

    @Test
    public void test_moved() throws ExecutionException, InterruptedException {
        CompletableFuture<byte[]> future = new CompletableFuture<>();

        handler.handle(request, null, future);
        future.complete("-MOVED 3333 192.168.100.101:7001\r\n".getBytes());
        Assert.assertArrayEquals(future.get(), "-MOVED 3333 192.168.100.201:7001\r\n".getBytes());
    }

    @Test
    public void test_ask() throws ExecutionException, InterruptedException {
        CompletableFuture<byte[]> future = new CompletableFuture<>();

        handler.handle(request, null, future);
        future.complete("-ASK 3333 192.168.100.101:7001\r\n".getBytes());
        Assert.assertArrayEquals(future.get(), "-ASK 3333 192.168.100.201:7001\r\n".getBytes());
    }

}
