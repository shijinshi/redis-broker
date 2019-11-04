package cn.shijinshi.redis.control.controller;

import cn.shijinshi.redis.common.param.HostAndPort;

import java.util.Set;

/**
 * BrokerDetector的回调接口
 *
 * @author Gui Jiahai
 */
interface BrokerDetectorCallback {

    /**
     * 当RedisDetector检测到broker列表的变动时，
     * 会通过这个接口通知用户
     *
     * @param active 若active为true，则表示当前节点获得controller的权利
     *              若为false，则表示当前节点没有controller的权利
     * @param epoch 表示当前节点获取controller时的纪元，若active为false，则永远为-1
     * @param addressSet 表示broker列表，若active为false，则为null或者空
     */
    void brokerChanged(boolean active, long epoch, Set<HostAndPort> addressSet);
}
