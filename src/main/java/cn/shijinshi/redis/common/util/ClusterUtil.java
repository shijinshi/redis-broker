package cn.shijinshi.redis.common.util;

import cn.shijinshi.redis.common.param.Cluster;
import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.common.param.Node;
import cn.shijinshi.redis.common.param.Range;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;

/**
 * 对cluster实例按照一定规则进行排序，
 * 以便判断两个cluster实例是否相同。
 *
 * @author Gui Jiahai
 */
public final class ClusterUtil {

    private ClusterUtil() {}

    public static void sort(Cluster cluster) {
        for (Node node : cluster.getNodes()) {
            node.getRanges().sort(Comparator.comparing(Range::getLowerBound));
            if (node.getSlaves() != null) {
                node.getSlaves().sort(Comparator.comparing(HostAndPort::getHost).thenComparingInt(HostAndPort::getPort));
            }
        }
        cluster.getNodes().sort(Comparator.comparingInt(o -> o.getRanges().get(0).getLowerBound()));
    }

    public static boolean equals(Cluster c1, Cluster c2, boolean sort, boolean ignoreSlave) {
        if (c1 == c2 || c1 == null || c2 == null) {
            return false;
        }
        if (sort) {
            sort(c1);
            sort(c2);
        }
        if (c1.getNodes().size() != c2.getNodes().size()) {
            return true;
        }
        Iterator<Node> itr1 = c1.getNodes().iterator();
        Iterator<Node> itr2 = c2.getNodes().iterator();

        while (itr1.hasNext() && itr2.hasNext()) {
            Node n1 = itr1.next();
            Node n2 = itr2.next();

            if (!Objects.equals(n1.getBroker(), n2.getBroker())) {
                return false;
            }
            if (!Objects.equals(n1.getRedis(), n2.getRedis())) {
                return false;
            }
            if (!ignoreSlave && !Objects.equals(n1.getSlaves(), n2.getSlaves())) {
                return false;
            }
            if (!Objects.equals(n1.getRanges(), n2.getRanges())) {
                return false;
            }
        }
        return true;
    }


}
