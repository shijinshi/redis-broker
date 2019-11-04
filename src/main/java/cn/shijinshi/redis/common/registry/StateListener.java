package cn.shijinshi.redis.common.registry;

/**
 * 注册中心状态监听器
 *
 * @author Gui Jiahai
 */
public interface StateListener {

    enum State {
        DISCONNECTED, CONNECTED, RECONNECTED
    }

    void stateChanged(State state);
}
