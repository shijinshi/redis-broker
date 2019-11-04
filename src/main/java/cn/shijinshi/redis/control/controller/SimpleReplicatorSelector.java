package cn.shijinshi.redis.control.controller;

import cn.shijinshi.redis.common.param.Cluster;
import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.common.param.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * 简易的选择器
 *
 * @author Gui Jiahai
 */
public class SimpleReplicatorSelector implements ReplicatorSelector {

    private static final Logger logger = LoggerFactory.getLogger(SimpleReplicatorSelector.class);

    @Override
    public HostAndPort select(Set<HostAndPort> brokerSet, Cluster brokerCluster) {

        logger.info("start select replicator, brokerSet: {}, brokerCluster:  {}", brokerSet, brokerCluster);

        Set<HostAndPort> masters = new HashSet<>();
        Set<HostAndPort> slaves = new HashSet<>();

        if (brokerCluster != null) {
            for (Node node : brokerCluster.getNodes()) {
                masters.add(node.getBroker());
                if (node.getSlaves() != null) {
                    slaves.addAll(node.getSlaves());
                }
            }
        }

        Set<HostAndPort> unused = new HashSet<>(brokerSet);
        unused.removeAll(masters);
        unused.removeAll(slaves);

        HostAndPort result = null;

        /*
          如果有空闲的节点，就从空闲的节点中选取。
          如果有slaves节点，就从slaves节点中选取。
          最后，只能从masters中选取了。
         */
        if (!unused.isEmpty()) {
            result = unused.iterator().next();
        } else if (!slaves.isEmpty()) {
            result = slaves.iterator().next();
        } else if (!masters.isEmpty()) {
            result = masters.iterator().next();
        }

        logger.info("selected replicator: {}", result);

        return result;
    }
}
