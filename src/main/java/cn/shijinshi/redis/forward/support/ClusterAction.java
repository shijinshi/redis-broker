package cn.shijinshi.redis.forward.support;

import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.common.param.Cluster;
import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.common.param.Node;
import cn.shijinshi.redis.common.param.Range;
import cn.shijinshi.redis.common.protocol.RedisRequest;
import cn.shijinshi.redis.common.util.UnsafeByteString;
import cn.shijinshi.redis.control.broker.Broker;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static cn.shijinshi.redis.common.Constants.CRLF_CHAR;

/**
 * 本程序充当了代理，所以当客户端需要通过 CLUSTER SLOTS
 * 或者 CLUSTER NODES 获取集群信息时，应该返回的是Brokers
 * 的集群信息
 *
 * @author Gui Jiahai
 */
@Component("cluster")
@Lazy
public class ClusterAction implements Action {

    private static final UnsafeByteString SLOTS_SUB_COMMAND = new UnsafeByteString("slots".toLowerCase());
    private static final UnsafeByteString NODES_SUB_COMMAND = new UnsafeByteString("nodes".toLowerCase());

    private static final byte[] ERR_REPLY =
            "-CLUSTERDOWN The cluster is down cause unable to get cluster from broker \r\n".getBytes();

    private final Broker broker;
    private final HostAndPort localAddress;

    public ClusterAction(Broker broker, BrokerProperties properties) {
        this.broker = broker;
        this.localAddress = HostAndPort.create(properties.getAddress(), properties.getPort());
    }

    @Override
    public Boolean apply(RedisRequest redisRequest, CompletableFuture<byte[]> future) {
        if (SLOTS_SUB_COMMAND.equals(redisRequest.getSubCommand())) {
            Cluster cluster = broker.getCluster();
            if (cluster == null) {
                future.complete(ERR_REPLY);
            } else {
                byte[] bytes = toClusterSlotsBytes(cluster);
                future.complete(bytes);
            }

        } else if (NODES_SUB_COMMAND.equals(redisRequest.getSubCommand())) {
            Cluster cluster = broker.getCluster();
            if (cluster == null) {
                future.complete(ERR_REPLY);
            } else {
                byte[] bytes = toClusterNodesBytes(cluster);
                future.complete(bytes);
            }
        } else {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }


    private byte[] toClusterSlotsBytes(Cluster brokerCluster) {
        StringBuilder builder = new StringBuilder("*").append(CRLF_CHAR);
        int size = 0;
        for (Node node : brokerCluster.getNodes()) {
            for (Range range : node.getRanges()) {
                size ++;
                builder.append('*').append(3 + (node.getSlaves() == null ? 0 : node.getSlaves().size())).append(CRLF_CHAR)
                        .append(':').append(range.getLowerBound()).append(CRLF_CHAR)
                        .append(':').append(range.getUpperBound()).append(CRLF_CHAR);

                builder.append("*2").append(CRLF_CHAR)
                        .append('$').append(node.getBroker().getHost().length()).append(CRLF_CHAR)
                        .append(node.getBroker().getHost()).append(CRLF_CHAR)
                        .append(':').append(node.getBroker().getPort()).append(CRLF_CHAR);

                if (node.getSlaves() != null) {
                    for (HostAndPort slave : node.getSlaves()) {
                        builder.append("*2").append(CRLF_CHAR)
                                .append('$').append(slave.getHost().length()).append(CRLF_CHAR)
                                .append(slave.getHost()).append(CRLF_CHAR)
                                .append(':').append(slave.getPort()).append(CRLF_CHAR);
                    }
                }
            }
        }
        builder.insert(1, size);
        return builder.toString().getBytes(StandardCharsets.UTF_8);
    }


    /*
07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected
67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002 master - 0 1426238316232 2 connected 5461-10922
292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003 master - 0 1426238318243 3 connected 10923-16383
6ec23923021cf3ffec47632106199cb7f496ce01 127.0.0.1:30005 slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 connected
824fe116063bc5fcf9f4ffd895bc17aee7731ac3 127.0.0.1:30006 slave 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 0 1426238317741 6 connected
e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001 myself,master - 0 0 1 connected 0-5460
 */
    private byte[] toClusterNodesBytes(Cluster brokerCluster) {
        StringBuilder builder = new StringBuilder();
        for (Node node : brokerCluster.getNodes()) {
            builder.append(display(node.getBroker(), null, node.getRanges()));
            if (node.getSlaves() != null) {
                for (HostAndPort slave : node.getSlaves()) {
                    builder.append(display(slave, node.getBroker(), node.getRanges()));
                }
            }
        }

        int len = builder.length();
        builder.insert(0, CRLF_CHAR).insert(0, len).insert(0, '$');
        builder.append(CRLF_CHAR);

        return builder.toString().getBytes(StandardCharsets.UTF_8);
    }

    private String display(HostAndPort broker, HostAndPort master, List<Range> ranges) {
        String myself = broker.equals(localAddress) ? "myself," : "";
        String role = master == null ? "master" : "slave";
        String masterId = master == null ? "-" : getId(master.format());

        String s1 = String.format("%s %s %s%s %s 0 0 0 connected", getId(broker.format()), broker.format(), myself, role, masterId);

        StringBuilder builder = new StringBuilder();
        for (Range range : ranges) {
            builder.append(' ').append(range.getLowerBound());
            if (range.getLowerBound() != range.getUpperBound()) {
                builder.append('-').append(range.getUpperBound());
            }
        }

        return s1 + builder.toString() + '\n';
    }

    private String getId(String address) {
        String md5 = md5(address.getBytes(StandardCharsets.UTF_8));
        return String.format("%s%s", md5, md5.substring(0, 8));
    }

    private String md5(byte[] src) {
        StringBuilder ret = new StringBuilder();
        try {
            MessageDigest md = MessageDigest.getInstance("md5");
            md.update(src);
            byte[] bytes = md.digest();
            for (byte b : bytes) {
                String t = Integer.toHexString(b & 255);
                if (t.length() == 1) {
                    ret.append("0");
                }
                ret.append(t);
            }
            return ret.toString();
        } catch (NoSuchAlgorithmException ignored) {
        }
        return "";
    }


}
