package cn.shijinshi.redis.control.controller;

import cn.shijinshi.redis.common.param.Cluster;
import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.common.registry.RegistryService;
import cn.shijinshi.redis.common.util.ExecutorUtil;
import cn.shijinshi.redis.common.util.SimpleThreadFactory;
import cn.shijinshi.redis.control.rpc.Indication;
import cn.shijinshi.redis.control.rpc.RpcHelper;
import cn.shijinshi.redis.control.rpc.State;
import cn.shijinshi.redis.control.rpc.Type;
import cn.shijinshi.redis.forward.RedisConnector;
import cn.shijinshi.redis.forward.client.AutoRedisConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 继承自 {@link BaseController}
 * 有以下功能：
 * 1、如果被当选为controller，则与所有Brokers建立网络连接，
 *   以便能发送指令，控制Broker。
 * 2、当收到最新的Broker和Redis的对应关系，需要及时的发送给所有Brokers。
 *
 * @author Gui Jiahai
 */
public class ClusterController extends BaseController {
    private static final Logger logger = LoggerFactory.getLogger(ClusterController.class);

    protected static final long scheduledIntervalMs = 3000;

    protected final RpcHelper rpcHelper;
    protected Map<HostAndPort, RedisConnector> brokerConnectors;
    protected Cluster brokerCluster;

    private final Set<HostAndPort> unstableBrokers = Collections.synchronizedSet(new HashSet<>());
    private final ScheduledExecutorService scheduledExecutor;
    private ScheduledFuture<?> scheduledTask;

    public ClusterController(RegistryService registryService, BrokerProperties properties, RpcHelper rpcHelper) {
        super(registryService, properties);

        this.rpcHelper = Objects.requireNonNull(rpcHelper);
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(new SimpleThreadFactory("controller-scheduler", true));
    }

    @Override
    public void init() {
        super.init();
    }

    @Override
    public synchronized void activate(long epoch) {
        super.activate(epoch);
    }

    @Override
    public synchronized void update(Set<HostAndPort> brokerSet) {
        logger.info(">>> Brokers changed: {}", brokerSet);
        recreateBrokers(brokerSet);
    }

    @Override
    public synchronized void update(Cluster brokerCluster) {
        this.brokerCluster = brokerCluster;

        Indication indication = new Indication(getEpoch(), Type.CLUSTER);
        indication.setCluster(brokerCluster);
        broadcastClusterInfo(indication);
    }

    @Override
    public synchronized void deactivate() {
        super.deactivate();
        broadcastClusterInfo(null);
        releaseInvalidBrokers();
    }

    private void broadcastClusterInfo(Indication indication) {
        if (scheduledTask != null) {
            scheduledTask.cancel(true);
            scheduledTask = null;
        }
        if (indication == null || scheduledExecutor.isShutdown()) {
            return;
        }

        if (!scheduledExecutor.isShutdown()) {
            /*
            一般来讲，如果Broker收到了Indication就应该可以了。
            但是为了保证可靠性，这里采用了定时的不断发送。
             */
            scheduledTask = scheduledExecutor.scheduleWithFixedDelay(() -> {

                synchronized (ClusterController.this) {
                    if (brokerConnectors == null || brokerConnectors.isEmpty()) {
                        return;
                    }

                    Iterator<Map.Entry<HostAndPort, RedisConnector>> iterator = brokerConnectors.entrySet().iterator();
                    while (iterator.hasNext() && !Thread.interrupted()) {
                        Map.Entry<HostAndPort, RedisConnector> entry = iterator.next();

                        HostAndPort broker = entry.getKey();
                        RedisConnector connector = entry.getValue();

                        if (connector.isClosed()) {
                            continue;
                        }

                        rpcHelper.sendIndication(indication, connector)
                                .whenComplete((answer, throwable) -> {
                                    if (throwable != null) {
                                        logger.error("Failed to broadcast ClusterInfo to broker[{}]", broker, throwable);
                                        unstableBrokers.add(broker);
                                    } else {
                                        if (answer.getState() == State.BAD) {
                                            logger.error("Failed to broadcast ClusterInfo to broker[{}], cause received BAD Answer: {}",
                                                    broker, answer.getMessage());
                                            unstableBrokers.add(broker);
                                        } else {
                                            unstableBrokers.remove(broker);
                                        }
                                    }
                                });
                    }
                }

            }, 0, scheduledIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 创建与所有Brokers的连接
     */
    private void recreateBrokers(Set<HostAndPort> brokerSet) {
        Map<HostAndPort, RedisConnector> oldBrokers = this.brokerConnectors;
        Map<HostAndPort, RedisConnector> newBrokers = new HashMap<>();

        if (brokerSet != null) {
            for (HostAndPort address : brokerSet) {
                RedisConnector connector;
                if (oldBrokers == null || (connector = oldBrokers.get(address)) == null) {
                    connector = new AutoRedisConnector(address);
                    logger.info("Created broker connector: {}", address);
                } else {
                    oldBrokers.remove(address);
                }
                newBrokers.put(address, connector);
            }
        }
        releaseInvalidBrokers();
        this.brokerConnectors = newBrokers;
    }

    /**
     * 释放连接
     */
    private void releaseInvalidBrokers() {
        Map<HostAndPort, RedisConnector> oldBrokers = this.brokerConnectors;
        this.brokerConnectors = null;

        if (oldBrokers != null) {
            for (Map.Entry<HostAndPort, RedisConnector> entry : oldBrokers.entrySet()) {
                Closeable c;
                if ((c = entry.getValue()) != null) {
                    try {
                        c.close();
                    } catch (IOException ignored) {}
                }
                logger.info("Released broker connector: {}", entry.getKey());
            }
        }
    }

    public synchronized void destroy() {
        super.destroy();
        ExecutorUtil.shutdown(scheduledExecutor, 0, null);
        releaseInvalidBrokers();
    }

}
