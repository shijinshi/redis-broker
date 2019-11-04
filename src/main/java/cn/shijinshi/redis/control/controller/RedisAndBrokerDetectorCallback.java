package cn.shijinshi.redis.control.controller;

import cn.shijinshi.redis.common.param.Cluster;
import cn.shijinshi.redis.common.param.HostAndPort;

import java.util.Set;

/**
 * {@link RedisAndBrokerDetector}的回调接口
 *
 * @author Gui Jiahai
 */
interface RedisAndBrokerDetectorCallback {

    /**
     * controller处于激活状态，
     * 意味着当前节点被选为controller
     */
    void activate(long epoch);

    /**
     * 收到最新的broker列表
     */
    void update(Set<HostAndPort> brokerSet);

    /**
     * 收到最新的Broker和Redis对应关系
     */
    void update(Cluster newBrokerCluster);

    /**
     * controller处于未激活状态。
     * 如果当前节点与注册中心断开连接，其他节点会认为当前节点掉线。
     * 那么会有其他节点当选为controller。
     * 当恢复连接后，就会发现自己已经不是controller了。
     */
    void deactivate();
}
