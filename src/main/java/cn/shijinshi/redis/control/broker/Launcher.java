package cn.shijinshi.redis.control.broker;

import cn.shijinshi.redis.control.rpc.Answer;
import cn.shijinshi.redis.control.rpc.Indication;

/**
 * @author Gui Jiahai
 */
public interface Launcher {

    void start();

    default boolean support(Indication indication) {
        return false;
    }

    default Answer apply(Indication indication) {
        return Answer.bad("Unable to process indication of type: " + indication.getType());
    }

    void stop();

}
