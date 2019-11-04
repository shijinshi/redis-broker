package cn.shijinshi.redis.control.controller;

import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.common.registry.RegistryException;
import cn.shijinshi.redis.common.registry.RegistryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * 本程序的主要功能有 forward, sync, replication。
 * 一般生产上，使用的是Redis集群模式。如果本程序只有一个进程，
 * 那必然无法承担大的工作量，所以，本程序也才用了分布式的设计。
 *
 * Broker: 每个程序启动后的进程都可以称为Broker，Broker负责
 * 管理 forward, sync, replication 等功能。
 * Controller: 如果有多个Broker，那么Broker之前怎么协调工作的呢？
 * 所以，必须要从Brokers里面选择一个称为Controller。具体实现就是，
 * Brokers将自己的地址注册在注册中心，然后才用一致性算法，选择
 * 一个Broker作为Controller。
 *
 * Broker的forward功能，即将client的命令转发到redis节点上，然后将响应
 * 报文返回给client。为了设计上的简单，一个Broker只负责一个Redis节点。
 * 意味着，如果有三个Redis节点，那么至少要三个Broker。当然，多余的Broker
 * 将会啥也不干。那么，分配Broker和Redis节点的对应关系的任务就交给了Controller。
 *
 * Controller的任务：
 * 1、规划Redis和Broker的对应关系
 *   1) 监听Brokers，如果有Broker掉线或者上线，则考虑重新分配
 *   2) 从Redis集群获取Redis节点分布，当Redis节点分布发生变化，应该及时的重新分配
 *   3) 重新分配的对应关系，Controller需要及时的发送到所有Brokers
 *
 * 2、Controller要负责启动replication，即复制功能。
 *   当需要把Redis节点（或者集群）数据复制到另一个Redis节点（或者集群）时，
 *   需要Controller去协调 sync 和 replication。
 *
 *
 * @author Gui Jiahai
 */
public abstract class BaseController implements RedisAndBrokerDetectorCallback {
    private static final Logger logger = LoggerFactory.getLogger(BaseController.class);

    protected final RegistryService registryService;
    protected final BrokerProperties properties;

    /**
     * controller是个逻辑概念。
     * 当active为true时，表示controller是激活状态，
     * 也就是当前的进程充当controller。
     * 如果active为false，表示controller是未激活状态。
     */
    private boolean active = false;

    /**
     * 由于Controller是通过注册中心选举出来的。那假设如果网络故障，
     * 整个分布式系统有可能会出现两个Controller。
     * 所以，每个Controller都必须有一个epoch，这个epoch必须是唯一且
     * 自增的，也就是说，如果同时存在两个controller， controllerA和controllerB
     * 如果controllerA在controllerB之前选举出来的，那么controllerA的epoch就会小于controllerB的epoch。
     *
     * 然后，当Brokers收到来自controller的指令(Indication)，那么会根据epoch，
     * 判断命令是否过期。所以，如果出现两个controller，Brokers会自动忽略比较早的controller。
     */
    private long epoch;

    private RedisAndBrokerDetector redisAndBrokerDetector;

    public BaseController(RegistryService registryService, BrokerProperties properties) {
        this.registryService = Objects.requireNonNull(registryService);
        this.properties = Objects.requireNonNull(properties);
    }

    public void init() {
        try {
            if (!this.registryService.checkExists(properties.getBrokersPath())) {
                this.registryService.create(properties.getBrokersPath(), false, false);
            }
        } catch (RegistryException e) {
            throw new IllegalStateException(e);
        }
        this.redisAndBrokerDetector = new RedisAndBrokerDetector(registryService, properties, this);
    }

    @Override
    public void activate(long epoch) {
        logger.info(">>> Controller is activated, and the latest epoch is {}", epoch);
        this.epoch = epoch;
        this.active = true;
    }

    protected long getEpoch() {
        return epoch;
    }

    protected boolean isActive() {
        return active;
    }


    @Override
    public void deactivate() {
        logger.info(">>> Controller is deactivated");
        active = false;
    }

    public void destroy() {
        if (this.redisAndBrokerDetector != null) {
            this.redisAndBrokerDetector.close();
        }
    }


}
