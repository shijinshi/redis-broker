package cn.shijinshi.redis.control.broker;

import cn.shijinshi.redis.common.param.Cluster;
import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.common.param.Node;
import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.common.registry.RegistryException;
import cn.shijinshi.redis.common.registry.RegistryService;
import cn.shijinshi.redis.common.registry.StateListener;
import cn.shijinshi.redis.control.rpc.Indication;
import cn.shijinshi.redis.control.rpc.State;
import cn.shijinshi.redis.control.rpc.Type;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.mockito.Mockito.*;

/**
 * @author Gui Jiahai
 */
@RunWith(MockitoJUnitRunner.class)
public class BrokerTest {

    @InjectMocks
    private Broker broker;

    @Mock
    private RegistryService registryService;
    @Mock
    private Launcher launcher;
    @Mock
    private BrokerProperties properties;
    @Mock
    private NodeListener listener;

    private volatile int count = 0;

    @Before
    public void setup() {
        broker.setLaunchers(Collections.singletonList(launcher));
        broker.addListener(listener);
        broker.setRetryMs(100);
        when(properties.getBrokersPath()).thenReturn("/redis/brokers");
        when(properties.getAddress()).thenReturn("192.168.10.56");
        when(properties.getPort()).thenReturn(6279);
    }

    @Test
    public void test_registry_retry() throws RegistryException {
        when(registryService.create(any(), anyBoolean(), anyBoolean())).then((Answer<String>) invocation -> {
            String path = invocation.getArgument(0);
            if (properties.getBrokersPath().equals(path)) {
                return path;
            }
            if (count == 0) {
                count = 1;
                throw new RegistryException();
            } else {
                return invocation.getArgument(0);
            }
        });
        broker.init();
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        verify(registryService, times(3)).create(any(), anyBoolean(), anyBoolean());
    }

    @Test
    public void test_registry_reconnect() throws RegistryException {
        broker.init();
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
        broker.stateChanged(StateListener.State.RECONNECTED);
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));

        verify(registryService, times(3)).create(any(), anyBoolean(), anyBoolean());
    }

    @Test
    public void test_launcher() {
        broker.init();
        broker.destroy();
        verify(launcher, times(1)).start();
        verify(launcher, times(1)).stop();
    }

    @Test
    public void test_answer_cluster() {
        Node node = new Node();
        node.setBroker(HostAndPort.create("192.168.10.56", 6279));
        node.setRedis(HostAndPort.create("192.168.10.56", 6379));

        Cluster cluster = new Cluster();
        cluster.setNodes(Collections.singletonList(node));

        Indication indication = new Indication(1, Type.CLUSTER);
        indication.setCluster(cluster);

        cn.shijinshi.redis.control.rpc.Answer answer = broker.answer(indication);
        Assert.assertNotNull(answer);
        Assert.assertEquals(answer.getState(), State.OK);

        verify(listener, times(1)).nodeChanged(node.getRedis());
    }

    @Test
    public void test_launcher_support() {
        cn.shijinshi.redis.control.rpc.Answer next = cn.shijinshi.redis.control.rpc.Answer.next();
        Indication indication = new Indication(10, Type.SYNC_DISCARD);

        when(launcher.support(any())).thenReturn(Boolean.TRUE);
        when(launcher.apply(any())).thenReturn(next);

        cn.shijinshi.redis.control.rpc.Answer reply = broker.answer(indication);
        Assert.assertEquals(reply, next);

        verify(launcher, times(1)).apply(indication);
    }

    @Test
    public void test_launcher_not_support() {
        when(launcher.support(any())).thenReturn(Boolean.FALSE);
        Indication indication = new Indication(10, Type.SYNC_DISCARD);
        cn.shijinshi.redis.control.rpc.Answer reply = broker.answer(indication);
        Assert.assertEquals(reply.getState(), State.BAD);
    }

}
