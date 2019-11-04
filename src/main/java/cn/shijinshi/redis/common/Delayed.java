package cn.shijinshi.redis.common;

import java.util.concurrent.TimeUnit;

/**
 * 延迟退出，用于优雅停机
 *
 * 在程序退出时，部分模块需要额外的时间来退出。
 * 比如同步模块需要尽可能的把队列中的消息写入到redis集群中。
 *
 *
 * @author Gui Jiahai
 */
public interface Delayed {

    void setDelay(long delay, TimeUnit timeUnit);

}
