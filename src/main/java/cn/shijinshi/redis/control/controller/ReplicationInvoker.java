package cn.shijinshi.redis.control.controller;

import cn.shijinshi.redis.common.Constants;
import cn.shijinshi.redis.common.param.Cluster;
import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.common.param.Node;
import cn.shijinshi.redis.common.param.Range;
import cn.shijinshi.redis.common.protocol.SlotHash;
import cn.shijinshi.redis.control.rpc.*;
import cn.shijinshi.redis.forward.RedisConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/*

状态流程图
 ---------------------------------------------------
            BROKERS             REPLICATOR
 ---------------------------------------------------
 1.         SYNC_PAUSE
 ---------------------------------------------------
 2.                             REPL_ACTIVE
 ---------------------------------------------------
 3.                             REPL_PING
 ---------------------------------------------------
 4.                             REPL_IN_COMPLETION
 ---------------------------------------------------
 5.         SYNC_DISCARD
 ---------------------------------------------------
 6.         SEND_TICK
 ---------------------------------------------------
 7.                             COMPLETION
 ----------------------------------------------------
 */
/**
 * 复制流程的控制器。
 *
 * 1、首先需要从Brokers中选择一个节点(Replicator)用于进行Replication(复制)工作
 * 2、命令所有的Brokers进入SYNC_PAUSE状态，即暂停Sync工作(即将磁盘日志中的请求发送到另一个Redis集群)
 * 3、待所有的Brokers进入SYNC_PAUSE状态后，命令Replicator开始复制工作(REPL_ACTIVE)
 * 4、replicator将数据复制到另一个Redis集群后，就进入AOF同步状态；
 *   这时候，Invoker通过Brokers给每个Redis节点写入一个TICK。
 * 5、写入TICK之后，当Replicator通过AOF同步遇到TICK之后，就停止Replication工作。
 * 6、写入TICK之后，所有的Brokers将抛弃磁盘日志中的请求，直到遇到TICK后，才开始正常同步工作。
 *
 *
 * Replicator和Sync模块都可以将数据同步到另一个Redis集群，所以，当Replicator开始同步数据，
 * 那么这时候Sync就应该暂停工作，否则数据就会重复写入。当Replicator写完数据后，会通过AOF
 * 继续实时同步数据。但是，这时候，同步数据工作应该要交接给Sync。所以，这里设置一个TICK，
 * 用于Replicator和Sync的工作交接点。当Replicator遇到TICK，则停止同步；当Sync遇到TICK，则
 * 开始同步。
 *
 *
 * 这里还有一个问题，Replicator复制期间，Sync需要暂停工作。那么当复制流程出问题了，就要恢复Sync的工作。
 * 可是，如果因为网络故障，导致命令发送失败，那么Sync一直处于暂停模式。
 * 这里的设计就是，当Sync进入SYNC_PAUSE状态后，Invoker就一直定时给Brokers发送SYNC_PING。
 * 如果Brokers在一定时间内没有收到PING，则应该主动恢复工作。
 * 当然，由于网络故障，Invoker无法联络到Brokers，也会采取相应的行动。
 *
 * 关于SYNC_PING 和 REPL_PING 还有一个功能。
 * 由于这里RPC的设计问题，只能是Controller主动发送命令给Broker。如果Broker状态发生改变，
 * Broker无法将消息发送给Controller。所以，Controller定时给Broker发送PING，Broker如果发生状态改变，
 * 就返回Answer.Type.NEXT给Controller。
 *
 * @author Gui Jiahai
 */
public class ReplicationInvoker extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationInvoker.class);

    private static final long waitMs = 3000;
    private static final long pingIntervalMs = Constants.PING_INTERVAL_MS;

    private final ReplicatorSelector replicatorSelector = new SimpleReplicatorSelector();
    private final RpcHelper rpcHelper;
    private final Map<HostAndPort, RedisConnector> brokerConnectors;
    private final Cluster brokerCluster;
    private final BiConsumer<ReplicationInvoker, Boolean> callback;
    private final long epoch;
    private final String tick;

    private Thread heartbeatThread;
    private volatile boolean heartbeatToNext = false;

    public ReplicationInvoker(RpcHelper rpcHelper, Map<HostAndPort, RedisConnector> brokerConnectors,
                              Cluster brokerCluster, long epoch, BiConsumer<ReplicationInvoker, Boolean> callback) {
        super("replication-invoker-thread");

        this.rpcHelper = Objects.requireNonNull(rpcHelper);
        this.brokerConnectors = new HashMap<>(brokerConnectors);
        this.brokerCluster = brokerCluster;
        this.callback = callback;
        this.epoch = epoch;
        this.tick = UUID.randomUUID().toString().replaceAll("-", "");
    }

    @Override
    public void run() {
        logger.info("Starting replication, epoch = {}, tick = {}", epoch, tick);

        HostAndPort replicator = null;
        RedisConnector replicatorConnector = null;

        boolean success = false;
        try {
            if (brokerConnectors == null || brokerConnectors.isEmpty()) {
                logger.error("Failed to process replication, cause brokerConnectors is empty");
                return;
            }
            if (brokerCluster == null || brokerCluster.getNodes() == null || brokerCluster.getNodes().isEmpty()) {
                logger.error("Failed to process replication, cause brokerCluster is null");
                return;
            }

            /*
            从Brokers中选出一个节点，作为Replicator
             */
            replicator = this.replicatorSelector.select(brokerConnectors.keySet(), brokerCluster);
            if (replicator == null) {
                logger.error("Failed to process replication, cause cannot select a broker as replicator");
                return;
            }

            logger.info("Selected replicator: {}", replicator);

            replicatorConnector = brokerConnectors.get(replicator);
            if (replicatorConnector == null) {
                logger.error("Failed to process replication, cause cannot get connector for broker: {}", replicator);
                return;
            }

            /*
            给Brokers发送Indication(SYNC_PAUSE)，使Brokers的sync模块进入暂停状态
             */
            if (!syncPause()) {
                logger.error("Failed to process replication, cause cannot make all brokers into SYNC_PAUSE");
                return;
            }

            /*
            开启定时任务，给Brokers发送心跳包
             */
            heartbeatThread = new Thread(this::heartbeat, "replication-invoker-heartbeat-thread");
            heartbeatThread.start();

            /*
            启动Replicator
             */
            if (!replActive(replicatorConnector, replicator)) {
                logger.error("Failed to process replication, cause cannot make replicator into REPL_ACTIVE");
                return;
            }

            /*
            定时给Replicator发送PING，如果Replicator完成RDB，进入AOF后，
            就返回Answer(NEXT)
             */
            if (!replPing(replicatorConnector, replicator)) {
                logger.error("Failed to process replication, cause it's error while ping to replicator");
                return;
            }

            /*
            给Replicator发送TICK，通知Replicator在AOF中，
            如果遇到TICK，则结束Replication
             */
            if (!replInCompletion(replicatorConnector, replicator)) {
                logger.error("Failed to process replication, cause cannot make replicator into REPL_IN_COMPLETION");
                return;
            }

            /*
            给Brokers发送TICK，通知Brokers，丢弃磁盘日志中所有的命令，
            直到遇到TICK，就恢复正常工作。
             */
            if (!syncDiscard()) {
                logger.error("Failed to process replication, cause cannot make brokers into SYNC_DISCARD");
                return;
            }

            /*
            将TICK通过Brokers发送给Redis节点
             */
            if (!sendTickToBrokers()) {
                logger.error("Failed to process replication, cause it's error while send tick to brokers");
                return;
            }

            /*
            给Replicator发送PING，等待Replicator收到TICK。
             */
            if (!replPing(replicatorConnector, replicator)) {
                logger.error("Failed to process replication, cause it's error while ping to replicator");
                return;
            }

            heartbeatToNext = true;

            logger.info("The data has been replicated to target redis. Now wait for all brokers to run normally.");

            /*
            等待Brokers恢复Sync
             */
            heartbeatThread.join();

            logger.info("The replication has been perfectly completed.");

            success = true;

        } catch (Throwable t) {
            logger.error("Failed to process replication", t);
        } finally {
            if (heartbeatThread != null && heartbeatThread.isAlive()) {
                heartbeatThread.interrupt();
            }

            try {
                if (!success) {
                    recover(replicator, replicatorConnector);
                }
            } finally {
                callback.accept(this, success);
            }
        }
    }

    private boolean syncPause() {
        logger.info("Starting SYNC_PAUSE");

        Indication indication = new Indication(epoch, Type.SYNC_PAUSE);

        for (Node node : brokerCluster.getNodes()) {
            if (Thread.interrupted())
                return false;

            RedisConnector connector = brokerConnectors.get(node.getBroker());
            if (connector == null) {
                logger.error("Cannot get connector of broker[{}]", node.getBroker());
                return false;
            }

            try {
                Answer answer = rpcHelper.sendIndication(indication, connector).get(waitMs, TimeUnit.MILLISECONDS);
                if (answer.getState() != cn.shijinshi.redis.control.rpc.State.OK) {
                    logger.error("Broker[{}] return NOT OK for SYNC_PAUSE: {}", node.getBroker(), answer);
                    return false;
                }
            } catch (InterruptedException e) {
                return false;
            } catch (Throwable t) {
                logger.error("Failed to send SYNC_PAUSE to broker[{}]", node.getBroker(), t);
                return false;
            }
        }

        return true;
    }

    private boolean replActive(RedisConnector connector, HostAndPort replicator) {
        logger.info("Starting REPL_ACTIVE");

        Indication indication = new Indication(epoch, Type.REPL_ACTIVE);
        try {
            Answer answer = rpcHelper.sendIndication(indication, connector).get(waitMs, TimeUnit.MILLISECONDS);
            if (answer.getState() != cn.shijinshi.redis.control.rpc.State.OK) {
                logger.error("Replicator[{}] return NOT OK for REPL_ACTIVE: {}", replicator, answer);
                return false;
            }
        } catch (InterruptedException e) {
            return false;
        } catch (Throwable t) {
            logger.error("Failed to send REPL_ACTIVE to replicator[{}]", replicator, t);
            return false;
        }
        return true;
    }

    private boolean replPing(RedisConnector connector, HostAndPort replicator) {
        logger.info("Starting REPL_PING");

        Indication indication = new Indication(epoch, Type.REPL_PING);
        while (true) {
            try {
                Thread.sleep(pingIntervalMs);
            } catch (InterruptedException e) {
                return false;
            }

            try {
                Answer answer = rpcHelper.sendIndication(indication, connector).get(waitMs, TimeUnit.MILLISECONDS);
                if (answer.getState() != cn.shijinshi.redis.control.rpc.State.OK) {
                    if (answer.getState() == cn.shijinshi.redis.control.rpc.State.NEXT) {
                        return true;
                    } else {
                        logger.error("Replicator[{}] return BAD for REPL_PING: {}", replicator, answer);
                        return false;
                    }
                }
            } catch (InterruptedException e) {
                return false;
            } catch (Throwable t) {
                logger.error("Failed to send REPL_PING to replicator[{}]", replicator, t);
                return false;
            }
        }
    }

    private boolean replInCompletion(RedisConnector connector, HostAndPort replicator) {
        logger.info("Starting REPL_IN_COMPLETION");

        Indication indication = new Indication(epoch, Type.REPL_IN_COMPLETION);
        indication.setMessage(tick);
        try {
            Answer answer = rpcHelper.sendIndication(indication, connector).get(waitMs, TimeUnit.MILLISECONDS);
            if (answer.getState() == cn.shijinshi.redis.control.rpc.State.OK) {
                return true;
            } else {
                logger.error("Replicator[{}] return BAD for REPL_IN_COMPLETION: {}", replicator, answer);
                return false;
            }
        } catch (InterruptedException e) {
            return false;
        } catch (Throwable t) {
            logger.error("Failed to send REPL_IN_COMPLETION to replicator[{}]", replicator, t);
            return false;
        }
    }

    private boolean syncDiscard() {
        logger.info("Starting SYNC_DISCARD");

        Indication indication = new Indication(epoch, Type.SYNC_DISCARD);
        indication.setMessage(tick);

        for (Node node : brokerCluster.getNodes()) {
            if (Thread.interrupted())
                return false;

            RedisConnector connector = brokerConnectors.get(node.getBroker());
            if (connector == null) {
                logger.error("Cannot get connector of broker[{}]", node.getBroker());
                return false;
            }

            try {
                Answer answer = rpcHelper.sendIndication(indication, connector).get(waitMs, TimeUnit.MILLISECONDS);
                if (answer.getState() != cn.shijinshi.redis.control.rpc.State.OK) {
                    logger.error("Broker[{}] return NOT OK for SYNC_DISCARD: {}", node.getBroker(), answer);
                    return false;
                }
            } catch (InterruptedException e) {
                return false;
            } catch (Throwable t) {
                logger.error("Failed to send SYNC_DISCARD to broker[{}]", node.getBroker(), t);
                return false;
            }
        }
        return true;
    }

    private boolean sendTickToBrokers() {
        logger.info("Starting send tick to master brokers");

        for (Node node : brokerCluster.getNodes()) {
            if (Thread.interrupted())
                return false;

            RedisConnector connector = brokerConnectors.get(node.getBroker());
            if (connector == null) {
                logger.error("Cannot get connector of broker[{}]", node.getBroker());
                return false;
            }
            try {
                byte[] request = tickRequest(node);
                CompletableFuture<byte[]> future = new CompletableFuture<>();
                connector.send(request, future);
                byte[] data = future.get(waitMs, TimeUnit.MILLISECONDS);
                if (data == null || data.length == 0) {
                    logger.error("Failed to send tickRequest to broker[{}], cause reply empty", node.getBroker());
                    return false;
                } else if (data[0] == '-'){
                    logger.error("Failed to send tickRequest to broker[{}], cause reply error", node.getBroker(), new String(data));
                    return false;
                }

            } catch (InterruptedException e) {
                return false;
            } catch (Throwable t) {
                logger.error("Failed to send tickRequest to broker[{}]", node.getBroker(), t);
                return false;
            }
        }

        return true;
    }

    private void heartbeat() {
        Indication indication = new Indication(epoch, Type.SYNC_PING);
        Set<HostAndPort> remains = new HashSet<>();
        brokerCluster.getNodes().forEach(node -> remains.add(node.getBroker()));

        while (!Thread.interrupted()) {
            Map<HostAndPort, CompletableFuture<Answer>> futureMap = new HashMap<>();
            for (Node node : brokerCluster.getNodes()) {
                HostAndPort broker = node.getBroker();
                if (!remains.contains(broker)) {
                    continue;
                }
                RedisConnector connector = brokerConnectors.get(broker);
                if (connector == null) {
                    logger.warn("Cannot get connector of broker[{}], ignored it", broker);
                    remains.remove(broker);
                    continue;
                }
                CompletableFuture<Answer> future = rpcHelper.sendIndication(indication, connector);
                futureMap.put(broker, future);
            }

            if (futureMap.isEmpty()) {
                return;
            }

            long left = waitMs;
            if (heartbeatToNext) {
                for (Map.Entry<HostAndPort, CompletableFuture<Answer>> entry : futureMap.entrySet()) {
                    long start = System.currentTimeMillis();
                    HostAndPort broker = entry.getKey();
                    CompletableFuture<Answer> future = entry.getValue();
                    try {
                        Answer answer = future.get(left, TimeUnit.MILLISECONDS);
                        if (answer.getState() != cn.shijinshi.redis.control.rpc.State.OK) {
                            remains.remove(broker);
                            if (answer.getState() == cn.shijinshi.redis.control.rpc.State.NEXT) {
                                logger.info("Broker[{}] run normally", broker);
                            } else if (answer.getState() == cn.shijinshi.redis.control.rpc.State.EXPIRE) {
                                logger.warn("Broker[{}] return EXPIRE for SYNC_PING, ignored it", broker);
                            } else {
                                logger.error("Broker[{}] return BAD for SYNC_PING: {}, ignored it", broker, answer);
                            }
                        }
                    } catch (InterruptedException e) {
                        return;
                    } catch (Throwable t) {
                        logger.warn("Failed to send SYNC_PING to broker[{}], ignored it", broker, t);
                        remains.remove(broker);
                    } finally {
                        left -= (System.currentTimeMillis() - start);
                    }
                }
            }

            if (remains.isEmpty()) {
                return;
            }

            try {
                long ms = pingIntervalMs - (waitMs - left);
                if (ms > 0) {
                    Thread.sleep(ms);
                }
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private byte[] tickRequest(Node node) throws InterruptedException {
        String t = getTick(node);
        String request = String.format("*4\r\n$5\r\nSETEX\r\n$%d\r\n%s\r\n$3\r\n600\r\n$%d\r\n%s\r\n",
                t.length(), t, t.length(), t);
        return request.getBytes();
    }

    private String getTick(Node node) throws InterruptedException {
        int count = 0;
        while (count < SlotHash.SLOT_COUNT) {
            if (Thread.interrupted())
                throw new InterruptedException();

            String t = tick + "-" + count;
            int slot = SlotHash.getSlot(t);
            for (Range range : node.getRanges()) {
                if (slot >= range.getLowerBound() && slot <= range.getUpperBound()) {
                    return t;
                }
            }
            count ++;
        }
        throw new IllegalStateException("Unable to work out the appropriate tic after " + SlotHash.SLOT_COUNT + "attempts");
    }

    private void recover(HostAndPort replicator, RedisConnector replicatorConnector) {
        logger.info("Starting recover");

        if (replicator == null && replicatorConnector == null) {
            return;
        }

        Indication indication = new Indication(epoch, Type.REPL_INTERRUPT);
        try {
            Answer answer = rpcHelper.sendIndication(indication, replicatorConnector).get(waitMs, TimeUnit.MILLISECONDS);
            if (answer.getState() == cn.shijinshi.redis.control.rpc.State.OK) {
                logger.info("Interrupted replication");
            } else {
                logger.warn("Replicator return NOT OK for REPL_INTERRUPT: {}", answer);
            }
        } catch (Throwable t) {
            logger.error("Failed to send REPL_INTERRUPT to broker[{}]", replicator, t);
        }


        indication = new Indication(epoch, Type.SYNC_RUN);
        for (Node node : brokerCluster.getNodes()) {
            RedisConnector connector = brokerConnectors.get(node.getBroker());
            if (connector == null) {
                continue;
            }
            try {
                Answer answer = rpcHelper.sendIndication(indication, connector).get(waitMs, TimeUnit.MILLISECONDS);
                if (answer.getState() != cn.shijinshi.redis.control.rpc.State.OK) {
                    logger.warn("Broker[{}] return NOT OK for SYNC_RUN: {}", node.getBroker(), answer);
                }
            } catch (Throwable t) {
                logger.error("Failed to send SYNC_RUN to broker[{}]", node.getBroker(), t);
            }
        }
    }

    /**
     * 取消复制
     */
    public void cancel() {
        this.interrupt();
    }


}
