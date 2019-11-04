package cn.shijinshi.redis.control.broker;

import cn.shijinshi.redis.common.Delayed;
import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.common.log.IndexLogger;

import java.util.concurrent.TimeUnit;

/**
 * 固定的同步模块启动器
 *
 * @author Gui Jiahai
 */
public class FixedSyncLauncher extends AbstractSyncLauncher implements Delayed {

    private volatile long delayMs = 0;

    public FixedSyncLauncher(IndexLogger indexLogger, BrokerProperties properties) {
        super(indexLogger, properties);
    }

    @Override
    public void start() {
        synchronizer.start();
    }

    @Override
    public void stop() {
        synchronizer.stop(delayMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void setDelay(long delay, TimeUnit timeUnit) {
        this.delayMs = timeUnit.toMillis(delay);
    }
}
