package cn.shijinshi.redis.common.prop;

import cn.shijinshi.redis.common.param.HostAndPort;

import java.util.List;

/**
 * @author Gui Jiahai
 */
public class RedisProperties {

    private List<HostAndPort> nodes;

    private HostAndPort hostAndPort;

    public List<HostAndPort> getNodes() {
        return nodes;
    }

    public void setNodes(List<HostAndPort> nodes) {
        this.nodes = nodes;
    }

    public HostAndPort getHostAndPort() {
        return hostAndPort;
    }

    public void setHostAndPort(HostAndPort hostAndPort) {
        this.hostAndPort = hostAndPort;
    }
}
