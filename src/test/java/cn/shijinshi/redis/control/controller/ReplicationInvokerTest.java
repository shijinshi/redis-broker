package cn.shijinshi.redis.control.controller;

import cn.shijinshi.redis.common.param.Cluster;
import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.control.rpc.RpcHelper;
import cn.shijinshi.redis.forward.RedisConnector;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * @author Gui Jiahai
 */
public class ReplicationInvokerTest {

    private ReplicationInvoker invoker;

    @Mock
    private RpcHelper helper;

    @Mock
    private RedisConnector connector1;
    @Mock
    private RedisConnector connector2;

    private Cluster brokerCluster;

    @Before
    public void setup() {
        Map<HostAndPort, RedisConnector> connectorMap = new HashMap<>();
        connectorMap.put(HostAndPort.create("192.168.10.56", 7001), connector1);
        connectorMap.put(HostAndPort.create("192.168.10.56", 7002), connector2);

        invoker = new ReplicationInvoker(helper, connectorMap, brokerCluster, 1, new BiConsumer<ReplicationInvoker, Boolean>() {
            @Override
            public void accept(ReplicationInvoker replicationInvoker, Boolean aBoolean) {
                System.out.println("completed: " + aBoolean);
            }
        });
        invoker.start();
    }

    @Test
    public void test() {




    }

}
