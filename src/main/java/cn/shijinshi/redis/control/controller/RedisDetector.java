package cn.shijinshi.redis.control.controller;

/**
 * 即时获取Redis集群的节点分布信息。
 * 如果Redis集群的节点分布发生变化，Controller必须要即时知道，
 * 以便重新规划Broker和Redis节点的对应关系。
 *
 * @author Gui Jiahai
 */
public interface RedisDetector {

    void start();

    void stop();

}
