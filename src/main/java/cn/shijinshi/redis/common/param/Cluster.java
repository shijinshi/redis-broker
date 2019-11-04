package cn.shijinshi.redis.common.param;

import java.io.Serializable;
import java.util.List;

/**
 * @author Gui Jiahai
 */
public class Cluster implements Serializable {

    private List<Node> nodes;

    public Cluster() {
    }

    public Cluster(List<Node> nodes) {
        this.nodes = nodes;
    }

    public List<Node> getNodes() {
        return nodes;
    }

    public void setNodes(List<Node> nodes) {
        this.nodes = nodes;
    }

    @Override
    public String toString() {
        return "Cluster{" +
                "nodes=" + nodes +
                '}';
    }
}
