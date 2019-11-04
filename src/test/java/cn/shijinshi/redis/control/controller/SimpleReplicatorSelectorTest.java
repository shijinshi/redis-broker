package cn.shijinshi.redis.control.controller;

import cn.shijinshi.redis.common.param.Cluster;
import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.common.param.Node;
import cn.shijinshi.redis.common.param.Range;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * @author Gui Jiahai
 */
public class SimpleReplicatorSelectorTest {

    private SimpleReplicatorSelector selector = new SimpleReplicatorSelector();

    @SuppressWarnings("all")
    @Test
    public void test() {

        Set<HostAndPort> brokerSet = new HashSet<>();

        List<Range> ranges = Arrays.asList(new Range(0, 5000), new Range(5001, 10000),
                new Range(10001, 16383));
        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < 3; i ++) {
            HostAndPort n = HostAndPort.create("192.168.10.56", i + 7001);

            brokerSet.add(n);

            Node node = new Node();
            node.setBroker(n);
            node.setRanges(Collections.singletonList(ranges.get(i)));
            nodes.add(node);
        }
        Cluster cluster = new Cluster(nodes);

        HostAndPort replicator;

        replicator = selector.select(brokerSet, cluster);
        Assert.assertNotNull(replicator);

        HostAndPort n4 = HostAndPort.create("192.168.10.56", 8080);
        brokerSet.add(n4);

        replicator = selector.select(brokerSet, cluster);
        Assert.assertEquals(replicator, n4);



    }
}
