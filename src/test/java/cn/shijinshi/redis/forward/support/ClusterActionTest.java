package cn.shijinshi.redis.forward.support;

import cn.shijinshi.redis.common.param.Cluster;
import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.common.param.Node;
import cn.shijinshi.redis.common.param.Range;
import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.common.protocol.RedisCodec;
import cn.shijinshi.redis.common.protocol.RedisRequest;
import cn.shijinshi.redis.common.util.UnsafeByteString;
import cn.shijinshi.redis.control.broker.Broker;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

/**
 * @author Gui Jiahai
 */
@RunWith(MockitoJUnitRunner.class)
public class ClusterActionTest {

    private ClusterAction action;

    @Mock
    private Broker broker;

    @Before
    public void setup() {
        BrokerProperties properties = new BrokerProperties();
        properties.setAddress("192.168.100.201");
        properties.setPort(7002);

        action = new ClusterAction(broker, properties);

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
    public void test_slots() throws ExecutionException, InterruptedException, IOException {
        RedisRequest request = new RedisRequest(null, new UnsafeByteString("cluster"), new UnsafeByteString("slots"));

        CompletableFuture<byte[]> future;
        Boolean result;
        byte[] bytes;
        ByteArrayInputStream input;
        byte[] reply;

        future = new CompletableFuture<>();
        result = action.apply(request, future);
        Assert.assertTrue(result);
        bytes = future.get();
        input = new ByteArrayInputStream(bytes);
        reply = RedisCodec.decodeReply(input);
        Assert.assertNotNull(reply);

        when(broker.getCluster()).thenReturn(null);
        future = new CompletableFuture<>();
        action.apply(request, future);
        Assert.assertTrue(result);
        bytes = future.get();
        input = new ByteArrayInputStream(bytes);
        reply = RedisCodec.decodeReply(input);
        Assert.assertNotNull(reply);
    }

    @Test
    public void test_nodes() throws ExecutionException, InterruptedException, IOException {
        RedisRequest request = new RedisRequest(null, new UnsafeByteString("cluster"), new UnsafeByteString("nodes"));

        CompletableFuture<byte[]> future;
        Boolean result;
        byte[] bytes;
        ByteArrayInputStream input;
        byte[] reply;

        future = new CompletableFuture<>();
        result = action.apply(request, future);
        Assert.assertTrue(result);
        bytes = future.get();
        input = new ByteArrayInputStream(bytes);
        reply = RedisCodec.decodeReply(input);
        Assert.assertNotNull(reply);

        when(broker.getCluster()).thenReturn(null);
        future = new CompletableFuture<>();
        action.apply(request, future);
        Assert.assertTrue(result);
        bytes = future.get();
        input = new ByteArrayInputStream(bytes);
        reply = RedisCodec.decodeReply(input);
        Assert.assertNotNull(reply);
    }



}
