package cn.shijinshi.redis.control.broker;

import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.control.rpc.Answer;
import cn.shijinshi.redis.control.rpc.Indication;
import cn.shijinshi.redis.forward.BrokerServer;
import cn.shijinshi.redis.forward.Handler;

/**
 * Forward模块的启动器
 *
 * @author Gui Jiahai
 */
public class FixedForwardLauncher implements Launcher {

    private final BrokerServer server;

    public FixedForwardLauncher(BrokerProperties properties, Handler handler) {
        this.server = new BrokerServer(properties, handler);
    }

    public void start() {
        this.server.start();
    }

    @Override
    public boolean support(Indication indication) {
        return false;
    }

    @Override
    public Answer apply(Indication indication) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public void stop() {
        this.server.stop();
    }

}
