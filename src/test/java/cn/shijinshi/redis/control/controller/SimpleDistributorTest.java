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
public class SimpleDistributorTest {

    private SimpleDistributor distributor = new SimpleDistributor();

    @Test
    public void test() {

        List<Range> ranges = Arrays.asList(new Range(0, 5000), new Range(5001, 10000),
                new Range(10001, 16383));
        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < 3; i ++) {
            Node node = new Node();
            nodes.add(node);

            node.setRedis(HostAndPort.create("192.168.10.56", i + 7001));
            node.setRanges(Collections.singletonList(ranges.get(i)));
        }
        Cluster cluster = new Cluster(nodes);

        Set<HostAndPort> brokerSet = new HashSet<>();
        brokerSet.add(HostAndPort.create("192.168.10.57", 8001));
        brokerSet.add(HostAndPort.create("192.168.10.57", 8002));
        brokerSet.add(HostAndPort.create("192.168.10.57", 8003));
        brokerSet.add(HostAndPort.create("192.168.10.57", 8004));

        Cluster newCluster = distributor.distribute(cluster, null, brokerSet);
        Assert.assertEquals(newCluster.getNodes().size(), cluster.getNodes().size());
        System.out.println(newCluster);

        brokerSet.add(HostAndPort.create("192.168.10.57", 8000));
        newCluster = distributor.distribute(cluster, newCluster, brokerSet);
        System.out.println(newCluster);

    }

}
