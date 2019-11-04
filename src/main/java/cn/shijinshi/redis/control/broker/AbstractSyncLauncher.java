package cn.shijinshi.redis.control.broker;

import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.common.log.IndexLogger;
import cn.shijinshi.redis.sync.RequestSynchronizer;

/**
 * 同步模块的启动器
 * 这里要强调一点：在Replication模块开始复制数据时，同步模块也会配合工作。
 * 但是，如果网络发生故障，Controller的指令(Indication)没有发送过来，
 * 那这时候，同步模块应该要自动恢复工作。
 * 这里的策略是，controller一旦开启了复制工作，
 * 必须要不断地给synchronizer 发送心跳包，即 Indication(SYNC_PING)。
 * 如果，在一段时间内没有收到心跳包，则认为出故障了，synchronizer则继续工作。
 * 当然，controller这时候应该也发现了失去broker的连接了，即controller也探测到
 * 了网络故障，所以，也应该采取相应的策略，即取消复制。
 *
 * @author Gui Jiahai
 */
public abstract class AbstractSyncLauncher implements Launcher {

    protected final RequestSynchronizer synchronizer;

    public AbstractSyncLauncher(IndexLogger indexLogger, BrokerProperties properties) {
        this.synchronizer = new RequestSynchronizer(indexLogger, properties.getTarget());
    }

}
