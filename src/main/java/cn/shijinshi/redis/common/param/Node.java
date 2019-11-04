package cn.shijinshi.redis.common.param;

import java.io.Serializable;
import java.util.List;

/**
 * 用于表示broker集群和redis集群。
 *
 * @author Gui Jiahai
 */
public class Node implements Serializable {

    private HostAndPort broker;

    /**
     * 当表示Redis集群时，redis表示集群中的master节点。
     * 当表示Broker集群时，redis表示broker对应的redis节点。
     */
    private HostAndPort redis;

    private List<HostAndPort> slaves;

    private List<Range> ranges;

    public HostAndPort getBroker() {
        return broker;
    }

    public void setBroker(HostAndPort broker) {
        this.broker = broker;
    }

    public HostAndPort getRedis() {
        return redis;
    }

    public void setRedis(HostAndPort redis) {
        this.redis = redis;
    }

    public List<HostAndPort> getSlaves() {
        return slaves;
    }

    public void setSlaves(List<HostAndPort> slaves) {
        this.slaves = slaves;
    }

    public List<Range> getRanges() {
        return ranges;
    }

    public void setRanges(List<Range> ranges) {
        this.ranges = ranges;
    }

    @Override
    public String toString() {
        return "Node{" +
                "broker=" + broker +
                ", redis=" + redis +
                ", slaves=" + slaves +
                ", ranges=" + ranges +
                '}';
    }
}
