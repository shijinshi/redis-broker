package cn.shijinshi.redis.control.controller;

import cn.shijinshi.redis.common.param.Cluster;
import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.common.param.Node;
import cn.shijinshi.redis.common.param.Range;
import cn.shijinshi.redis.common.protocol.SlotHash;

import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 在Redis是单节点模式的情况。
 *
 * @author Gui Jiahai
 */
public class RedisStandaloneDetector implements RedisDetector {

    private final RedisDetectorCallback callback;
    private final Cluster redisCluster;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public RedisStandaloneDetector(HostAndPort hostAndPort, RedisDetectorCallback callback) {
        Node node = new Node();
        node.setRedis(hostAndPort);
        node.setRanges(Collections.singletonList(new Range(0, SlotHash.SLOT_COUNT - 1)));

        this.callback = Objects.requireNonNull(callback);

        Cluster cluster = new Cluster();
        cluster.setNodes(Collections.singletonList(node));
        this.redisCluster = cluster;
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            if (this.callback != null) {
                this.callback.redisChanged(this.redisCluster);
            }
        }
    }

    @Override
    public void stop() {
        started.set(false);
    }


}
