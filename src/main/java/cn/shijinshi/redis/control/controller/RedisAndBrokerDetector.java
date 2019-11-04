package cn.shijinshi.redis.control.controller;

import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.common.param.Cluster;
import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.common.registry.RegistryService;
import cn.shijinshi.redis.common.util.ClusterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * 在 {@link BaseController} 提过到，Controller的任务之一是，规划Redis和Broker的对应关系
 * 那么controller在什么时候需要重新规划这个对应关系呢？
 * 1、当Redis集群的节点分布发生变化；
 * 2、当Brokers列表发生变化。
 *
 * 此类的功能将RedisDetector和BrokerDetector两者结合一起，
 * 当探测到Redis或Brokers发生变化时，如果controller是active状态，
 * 那么就重新规划这个对应关系。
 *
 * @author Gui Jiahai
 */
public class RedisAndBrokerDetector implements BrokerDetectorCallback, RedisDetectorCallback {

    private static final Logger logger = LoggerFactory.getLogger(RedisAndBrokerDetector.class);

    private final RedisAndBrokerDetectorCallback redisAndBrokerDetectorCallback;

    private final RedisDetector redisDetector;
    private final BrokerDetector brokerDetector;
    private final Distributor distributor;

    private Set<HostAndPort> brokerSet;
    private volatile boolean active = false;
    private Cluster redisCluster;
    private Cluster blockerCluster;

    public RedisAndBrokerDetector(RegistryService registryService, BrokerProperties properties, RedisAndBrokerDetectorCallback redisAndBrokerDetectorCallback) {
        this.redisAndBrokerDetectorCallback = Objects.requireNonNull(redisAndBrokerDetectorCallback);
        this.distributor = new SimpleDistributor();
        this.redisDetector = createRedisDetector(properties);
        this.brokerDetector = createBrokerDetector(registryService, properties);
    }

    private RedisDetector createRedisDetector(BrokerProperties properties) {
        RedisDetector redisDetector;
        if (properties.getSource().getNodes() != null && !properties.getSource().getNodes().isEmpty()) {
            redisDetector = new RedisClusterDetector(properties.getSource().getNodes(), this);
        } else {
            redisDetector = new RedisStandaloneDetector(properties.getSource().getHostAndPort(), this);
        }
        return redisDetector;
    }

    private BrokerDetector createBrokerDetector(RegistryService registryService, BrokerProperties properties) {
        return new BrokerDetector(registryService, properties, this);
    }

    @Override
    public synchronized void brokerChanged(boolean active, long epoch, Set<HostAndPort> addressSet) {
        logger.info(">>> Brokers changed, active:{}, epoch:{}, set:{}", active, epoch, addressSet);

        if (!this.active && active) {
            this.active = true;
            this.redisAndBrokerDetectorCallback.activate(epoch);
            this.redisDetector.start();
        }

        if (this.active && !active) {
            this.active = false;
            this.redisDetector.stop();
            this.redisCluster = null;
            this.redisAndBrokerDetectorCallback.deactivate();
        }

        if (active) {
            this.brokerSet = Collections.unmodifiableSet(addressSet);
            this.redisAndBrokerDetectorCallback.update(brokerSet);
            distribute();
        } else {
            this.brokerSet = null;
        }
    }

    @Override
    public synchronized void redisChanged(Cluster redisCluster) {
        logger.info(">>> Redis cluster changed: {}", redisCluster);
        this.redisCluster = redisCluster;
        if (active) {
            distribute();
        }
    }

    /**
     * 重新规划Broker和Redis节点之间的对应关系
     */
    private void distribute() {
        if (this.redisCluster == null || this.brokerSet == null) {
            return;
        }

        Cluster newCluster = this.distributor.distribute(this.redisCluster, this.blockerCluster, brokerSet);
        if (newCluster == null) {
            return;
        }
        ClusterUtil.sort(newCluster);
        this.blockerCluster = newCluster;
        this.redisAndBrokerDetectorCallback.update(newCluster);
    }

    public void close() {
        this.brokerDetector.close();
        this.redisDetector.stop();
    }

}
