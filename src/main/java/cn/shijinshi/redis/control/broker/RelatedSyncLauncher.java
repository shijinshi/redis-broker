package cn.shijinshi.redis.control.broker;

import cn.shijinshi.redis.common.Constants;
import cn.shijinshi.redis.common.Delayed;
import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.common.log.IndexLogger;
import cn.shijinshi.redis.common.util.SimpleThreadFactory;
import cn.shijinshi.redis.control.rpc.Answer;
import cn.shijinshi.redis.control.rpc.Indication;
import cn.shijinshi.redis.control.rpc.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 同步模块的启动器
 * 这里要强调一点：在Replication模块开始复制数据时，同步模块也会配合工作。
 * 但是，如果网络发生故障，Controller的指令(Indication)没有发送过来，
 * 那这时候，同步模块应该要自动恢复工作。
 *
 * @author Gui Jiahai
 */
public class RelatedSyncLauncher extends AbstractSyncLauncher implements Delayed {

    private static final Logger logger = LoggerFactory.getLogger(RelatedSyncLauncher.class);

    private volatile long delayMs;
    private volatile ScheduledExecutorService scheduledExecutor;

    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * 收到下一次心跳包的截止时间。
     * 如果在这之前，没有收到心跳包，则认为网络故障。
     */
    private volatile long nextHeartbeat;

    public RelatedSyncLauncher(IndexLogger indexLogger, BrokerProperties properties) {
        super(indexLogger, properties);
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        synchronizer.start();
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor(new SimpleThreadFactory("request-synchronizer-ping", true));
        scheduledExecutor.scheduleWithFixedDelay(() -> {

            /*
            开启定时任务，不断检查心跳包，
            尽量恢复同步任务。

            思路：在broker集群进行replication时，
            会由controller通过rpc控制所有的broker。
            但是，考虑到分布式系统中，容易出现各种问题，
            controller在出错后，无法保证一定能恢复同步任务，
            使broker集群正常运行。
            所以，在replication期间，broker应该要自检，如果发现
            与controller断开连接，那么就应该主动恢复同步任务，
            保证业务系统能正常工作。

            具体实现：记录上次controller发送Indication的时间，
            如果在一段时间内，没有收到新的Indication，那么则
            认为broker集群发生了某种故障。
            对于controller，则应该定时发送SYNC_INDICATION，
            来维持连接。
             */

            if (System.currentTimeMillis() > nextHeartbeat) {
                synchronized (RelatedSyncLauncher.this) {
                    if (synchronizer.isTicked() || synchronizer.isPaused()) {
                        synchronizer.resume_();
                    }
                }
            }
        }, 0, Math.max(Constants.PING_INTERVAL_MS, 1000), TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean support(Indication indication) {
        Type type = indication.getType();
        return type == Type.SYNC_PAUSE || type == Type.SYNC_RUN
                || type == Type.SYNC_DISCARD || type == Type.SYNC_PING;
    }

    @Override
    public Answer apply(Indication indication) {
        logger.info("Apply indication: {}", indication);

        nextHeartbeat = System.currentTimeMillis() + Constants.PING_INTERVAL_MS * 3;

        if (!started.get()) {
            return Answer.bad("The request synchronizer has not started yet, please start it first");
        }

        switch (indication.getType()) {
            case SYNC_RUN:
                if (synchronizer.isPaused() || synchronizer.isTicked()) {
                    logger.warn("The request synchronizer is paused or ticked, but received indication[SYNC_RUN], so resume_ synchronizer by force");
                    synchronizer.resume_();
                }
                break;
            case SYNC_PAUSE:
                if (synchronizer.isPaused()) {
                    String msg = "The request synchronizer is already paused, please check it";
                    logger.error(msg);
                    return Answer.bad(msg);
                }
                synchronizer.pause();
                break;
            case SYNC_DISCARD:
                if (!synchronizer.isPaused()) {
                    String msg = "The request synchronizer is not paused, so unable to process SYNC_DISCARD";
                    logger.error(msg);
                    return Answer.bad(msg);
                }
                synchronizer.resumeTick(indication.getMessage().getBytes());
                break;
            case SYNC_PING:
                if (!synchronizer.isPaused() || !synchronizer.isTicked()) {
                    return Answer.next();
                }
                break;
            default:
                return Answer.bad("Unable to process indication of type: " + indication.getType());
        }
        return Answer.ok();
    }

    @Override
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        if (scheduledExecutor != null) {
            scheduledExecutor.shutdownNow();
            scheduledExecutor = null;
        }
        synchronizer.stop(delayMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void setDelay(long delay, TimeUnit timeUnit) {
        this.delayMs = timeUnit.toMillis(delay);
    }
}
