package cn.shijinshi.redis.control.controller;

import cn.shijinshi.redis.common.Constants;
import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.common.param.Cluster;
import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.common.registry.ChildListener;
import cn.shijinshi.redis.common.registry.RegistryException;
import cn.shijinshi.redis.common.registry.RegistryService;
import cn.shijinshi.redis.common.util.ClusterUtil;
import cn.shijinshi.redis.control.rpc.*;
import cn.shijinshi.redis.forward.RedisConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 继承自 {@link ClusterController}
 * ReplicationController 主要用于协调 forward, sync, replication 三者。
 * 复制功能的流程请参考 {@link ReplicationInvoker}
 *
 * 为了简化流程，在复制过程中，如果Broker集群或者Redis集群发生变动，则取消复制。
 * 目前，启动复制功能的入口是，在注册中心的节点上 {@link BrokerProperties#getReplicationPath}
 * 写入子节点 {@link Constants#REPLICATION_HOLD}。
 * 那为什么不向节点 {@link BrokerProperties#getReplicationPath} 写入数据，而是写入子节点呢？
 * 主要是为了达到分布式锁的功能。
 *
 * 当此类监听到了这一动作，即可启动复制流程。
 *
 * @author Gui Jiahai
 */
public class ReplicationController extends ClusterController implements ChildListener {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationController.class);

    private ReplicationInvoker replicationInvoker;

    public ReplicationController(RegistryService registryService, BrokerProperties properties, RpcHelper rpcHelper) {
        super(registryService, properties, rpcHelper);
    }

    @Override
    public void init() {
        super.init();
        try {
            if (!this.registryService.checkExists(properties.getReplicationPath())) {
                this.registryService.create(properties.getReplicationPath(), false, false);
            }
        } catch (RegistryException e) {
            throw new IllegalStateException(e);
        }
        registryService.addChildListener(properties.getReplicationPath(), this);
    }

    @Override
    public synchronized void childChanged(String path, List<String> children) {
        if (children == null || children.isEmpty() || !isActive()) {
            return;
        }

        for (String child : children) {
            if (Constants.REPLICATION_HOLD.equalsIgnoreCase(child)) {
                startReplication();
                break;
            }
        }
    }

    @Override
    public synchronized void activate(long epoch) {
        super.activate(epoch);
    }

    @Override
    public synchronized void update(Cluster brokerCluster) {
        /*
        在进行replication时，如果broker集群或者redis集群发生变化，
        则取消replication。（当zookeeper进行重连时，也会触发这一机制）

        但是，为了尽可能地完成replication，将当前的brokerCluster和新的brokerCluster
        进行对比，如果一致，则不需要取消replication。
         */
        if (replicationInvoker != null) {
            if (!ClusterUtil.equals(this.brokerCluster, brokerCluster, false, true)) {
                logger.warn("Checked the broker cluster info changed and will cancel replication");
                replicationInvoker.cancel();
            }
        }

        super.update(brokerCluster);
    }

    @Override
    public synchronized void update(Set<HostAndPort> brokerSet) {
        super.update(brokerSet);

        if (replicationInvoker == null) {
            logger.info("Brokers update, sending SYNC_RUN to all brokers: {}", brokerConnectors.keySet());

            Indication indication = new Indication(getEpoch(), Type.SYNC_RUN);
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
                                logger.error("Failed to send SYNC_RUN to broker[{}]", broker, throwable);
                            } else {
                                if (answer.getState() == State.BAD) {
                                    logger.warn("Failed to send SYNC_RUN to broker[{}], cause received BAD Answer: {}", broker, answer);
                                }
                            }
                        });
            }
        }
    }

    @Override
    public synchronized void deactivate() {
        super.deactivate();

        logger.error("Checked controller is deactivated and will cancel replication");
        if (replicationInvoker != null) {
            replicationInvoker.cancel();
        }
    }

    private synchronized void startReplication() {
        if (replicationInvoker != null) {
            logger.error("Replication is already started");
            return;
        }

        replicationInvoker = new ReplicationInvoker(rpcHelper, Collections.unmodifiableMap(brokerConnectors), brokerCluster, getEpoch(), this::callback);
        replicationInvoker.start();
    }

    private synchronized void callback(ReplicationInvoker invoker, Boolean success) {
        if (invoker == this.replicationInvoker) {
            logger.info("Replication is completed, and result: {}", success);
            String path = properties.getReplicationPath() + Constants.PATH_SEPARATOR + Constants.REPLICATION_HOLD;
            try {
                registryService.delete(path);
            } catch (Throwable t) {
                logger.error("Failed to delete REPLICATION_HOLD in registry center, please delete manually: {}", path, t);
            } finally {
                this.replicationInvoker = null;
            }
        }
    }

    @Override
    public synchronized void destroy() {
        logger.info("Controller destroy ...");
        super.destroy();
    }
}
