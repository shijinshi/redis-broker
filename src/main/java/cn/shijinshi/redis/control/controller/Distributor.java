package cn.shijinshi.redis.control.controller;

import cn.shijinshi.redis.common.param.Cluster;
import cn.shijinshi.redis.common.param.HostAndPort;

import java.util.Set;

/**
 * Broker和Redis节点对应关系的分配器。
 * 实现类需要根据当前redis集群、之前的Broker和Redis的对应关系、当前Broker列表
 * 来进行分配。
 *
 * @author Gui Jiahai
 */
public interface Distributor {

    /**
     * @param redisCluster 最新的Redis集群信息
     * @param oldBrokerCluster 之前的Broker和Redis的对应关系
     * @param brokerSet 最新的Broker列表
     * @return Broker和Redis的对应关系
     */
    Cluster distribute(Cluster redisCluster, Cluster oldBrokerCluster, Set<HostAndPort> brokerSet);

}
