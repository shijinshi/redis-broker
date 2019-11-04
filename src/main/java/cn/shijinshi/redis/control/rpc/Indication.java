package cn.shijinshi.redis.control.rpc;

import cn.shijinshi.redis.common.param.Cluster;

import java.io.Serializable;
import java.util.Objects;

/**
 * 参考 {@link cn.shijinshi.redis.control.controller.ReplicationInvoker}
 *
 * @author Gui Jiahai
 */
public class Indication implements Serializable {

    private long epoch;
    private Type type;
    private Cluster cluster;
    private String message;

    public Indication() {}

    public Indication(long epoch, Type type) {
        this.epoch = epoch;
        this.type = Objects.requireNonNull(type);
    }

    public long getEpoch() {
        return epoch;
    }

    public Indication setEpoch(long epoch) {
        this.epoch = epoch;
        return this;
    }

    public Type getType() {
        return type;
    }

    public Indication setType(Type type) {
        this.type = type;
        return this;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public Indication setCluster(Cluster cluster) {
        this.cluster = cluster;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public Indication setMessage(String message) {
        this.message = message;
        return this;
    }

    @Override
    public String toString() {
        return "Indication{" +
                "epoch=" + epoch +
                ", type=" + type +
                ", cluster=" + cluster +
                ", message='" + message + '\'' +
                '}';
    }
}
