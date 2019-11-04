package cn.shijinshi.redis.control.controller;

import cn.shijinshi.redis.common.param.Cluster;
import cn.shijinshi.redis.common.param.HostAndPort;

import java.util.Set;

/**
 * 从Brokers中选择一个节点进行Replication工作。
 *
 * @author Gui Jiahai
 */
public interface ReplicatorSelector {

    HostAndPort select(Set<HostAndPort> brokerSet, Cluster brokerCluster);

}
