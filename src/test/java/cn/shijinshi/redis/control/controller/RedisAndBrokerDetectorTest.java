package cn.shijinshi.redis.control.controller;

import cn.shijinshi.redis.common.param.Cluster;
import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.common.param.Node;
import cn.shijinshi.redis.common.param.Range;
import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.common.registry.RegistryService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static org.mockito.Mockito.*;

/**
 * @author Gui Jiahai
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(RedisAndBrokerDetector.class)
@PowerMockIgnore("javax.management.*")
public class RedisAndBrokerDetectorTest {

    private RedisAndBrokerDetector detector;

    @Mock
    private RedisStandaloneDetector redisDetector;
    @Mock
    private BrokerDetector brokerDetector;
    @Mock
    private RegistryService registryService;

    @Before
    public void setup() throws Exception {
        PowerMockito.doNothing().when(redisDetector).start();
        PowerMockito.doNothing().when(redisDetector).stop();

        PowerMockito.whenNew(RedisStandaloneDetector.class).withAnyArguments().thenReturn(redisDetector);
        PowerMockito.whenNew(BrokerDetector.class).withAnyArguments().thenReturn(brokerDetector);

        BrokerProperties properties = new BrokerProperties();
        properties.getSource().setHostAndPort(HostAndPort.create("102.168.10.56", 7001));
        properties.setAddress("192.168.10.56");
        properties.setPort(6279);


        detector = new RedisAndBrokerDetector(registryService, properties, new RedisAndBrokerDetectorCallback() {
            @Override
            public void activate(long epoch) {
                System.out.println("activate: " + epoch);
            }

            @Override
            public void update(Set<HostAndPort> brokerSet) {
                System.out.println("brokerSet: " + brokerSet);
            }

            @Override
            public void update(Cluster newBrokerCluster) {
                System.out.println("cluster: " + newBrokerCluster);
            }

            @Override
            public void deactivate() {
                System.out.println("deactivate");
            }
        });
    }

    @Test
    public void test() {
        List<Range> ranges = Arrays.asList(new Range(0, 5000), new Range(5001, 10000),
                new Range(10001, 16383));
        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < 3; i ++) {
            Node node = new Node();
            node.setRedis(HostAndPort.create("192.168.10.56", i + 7001));
            node.setRanges(Collections.singletonList(ranges.get(i)));
            nodes.add(node);
        }
        Cluster cluster = new Cluster(nodes);

        Set<HostAndPort> brokerSet = new HashSet<>();
        brokerSet.add(HostAndPort.create("192.168.10.57", 8001));
        brokerSet.add(HostAndPort.create("192.168.10.57", 8002));
        brokerSet.add(HostAndPort.create("192.168.10.57", 8003));

        detector.redisChanged(cluster);
        detector.brokerChanged(true, 1, brokerSet);
        verify(redisDetector, times(1)).start();
        verify(redisDetector, times(0)).stop();

        detector.brokerChanged(true, 2, brokerSet);
        verify(redisDetector, times(1)).start();
        verify(redisDetector, times(0)).stop();

        detector.brokerChanged(false, -1, null);
        verify(redisDetector, times(1)).start();
        verify(redisDetector, times(1)).stop();
    }

    @After
    public void after() {
        detector.close();
    }

}
