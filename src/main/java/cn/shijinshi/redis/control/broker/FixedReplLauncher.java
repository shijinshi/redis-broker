package cn.shijinshi.redis.control.broker;

import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.replicate.ReplicationJob;

/**
 * 固定的复制模块启动器
 *
 * 实现类比较简单，只有启动和停止功能。
 * {@link RelatedReplLauncher} 可以接收Controller 的Indication，
 * 进行各种动作。
 *
 * 但是，如果只是希望本程序作为复制模块运作，那么可以使用 FixedReplLauncher
 *
 * @author Gui Jiahai
 */
public class FixedReplLauncher implements Launcher {

    private final BrokerProperties properties;
    private ReplicationJob replicationJob;

    public FixedReplLauncher(BrokerProperties properties) {
        this.properties = properties;
    }

    @Override
    public void start() {
        if (replicationJob != null) {
            throw new IllegalStateException("Failed to process replication, cause replication already exists");
        }
        this.replicationJob = new ReplicationJob(properties.getSource(), properties.getTarget(), null);
        this.replicationJob.start();
    }

    @Override
    public void stop() {
        if (replicationJob != null) {
            this.replicationJob.interrupt();
        }
    }

}
