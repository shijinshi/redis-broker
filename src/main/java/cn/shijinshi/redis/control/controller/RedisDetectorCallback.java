package cn.shijinshi.redis.control.controller;

import cn.shijinshi.redis.common.param.Cluster;

/**
 * @author Gui Jiahai
 */
public interface RedisDetectorCallback {

    void redisChanged(Cluster redisCluster);
}
