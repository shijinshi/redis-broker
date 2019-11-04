package cn.shijinshi.redis.common.protocol;

import cn.shijinshi.redis.common.param.Cluster;
import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.common.param.Node;
import cn.shijinshi.redis.common.param.Range;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.*;

/**
 * Redis集群信息探测器。
 *
 * 创建Socket，连接Redis节点，然后发送 CLUSTER SLOTS 请求，
 * 获取Redis Cluster的集群信息。
 *
 * @author Gui Jiahai
 */
public class RedisProbe implements Closeable {

    private static final byte[] CLUSTER_SLOTS = "*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n".getBytes();

    private final Socket socket;

    private final InputStream input;
    private final OutputStream output;

    public RedisProbe(HostAndPort hostAndPort) throws IOException {
        this.socket = new Socket();
        this.socket.connect(new InetSocketAddress(hostAndPort.getHost(), hostAndPort.getPort()), 3000);
        this.socket.setKeepAlive(true);
        this.socket.setTcpNoDelay(true);
        this.input = socket.getInputStream();
        this.output = socket.getOutputStream();
    }

    public Cluster discover() throws IOException {
        output.write(CLUSTER_SLOTS);
        output.flush();

        byte[] reply = RedisCodec.decodeReply(input);
        if (reply == null || reply.length == 0) {
            throw new IOException("Failed to get reply");
        }

        if (reply[0] == '-') {
            throw new IOException(new String(reply, 1, reply.length - 3));
        } else if (reply[0] != '*') {
            throw new IOException("Unknown byte: " + reply[0]);
        }

        List<Node> nodes = parse(new String(reply));
        return reduce(nodes);
    }

    private Cluster reduce(List<Node> nodes) {
        Map<HostAndPort, Node> nodeMap = new LinkedHashMap<>();
        for (Node node : nodes) {
            if (nodeMap.containsKey(node.getRedis())) {
                Node oldNode = nodeMap.get(node.getRedis());
                List<Range> ranges = oldNode.getRanges();
                if (ranges instanceof ArrayList) {
                    ranges.addAll(node.getRanges());
                } else {
                    ranges = new ArrayList<>();
                    ranges.addAll(oldNode.getRanges());
                    ranges.addAll(node.getRanges());
                    oldNode.setRanges(ranges);
                }
            } else {
                nodeMap.put(node.getRedis(), node);
            }
        }

        List<Node> newNodes = new ArrayList<>(nodes.size());
        for (Map.Entry<HostAndPort, Node> entry : nodeMap.entrySet()) {
            newNodes.add(entry.getValue());
        }
        return new Cluster(newNodes);
    }

    private List<Node> parse(String reply) {
        String[] subs = reply.split("\r\n");

        List<Node> nodes = new ArrayList<>();

        int line = 1;
        while (subs.length > line) {
            Node node = new Node();
            nodes.add(node);

            int num = Integer.parseInt(subs[line ++].substring(1));
            int lower = Integer.parseInt(subs[line ++].substring(1));
            int upper = Integer.parseInt(subs[line ++].substring(1));

            Range range = new Range(lower, upper);
            node.setRanges(Collections.singletonList(range));

            List<HostAndPort> slaves = new ArrayList<>();
            for (int i = 0; i < num - 2; i ++) {
                int rows = Integer.parseInt(subs[line ++].substring(1));
                line ++;
                String host =subs[line ++];
                int port = Integer.parseInt(subs[line ++].substring(1));

                if (i == 0) {
                    node.setRedis(new HostAndPort(host, port));
                } else {
                    slaves.add(new HostAndPort(host, port));
                }

                if (rows == 3) {
                    line += 2;
                }
            }

            if (!slaves.isEmpty()) {
                node.setSlaves(slaves);
            }
        }

        return nodes;
    }

    @Override
    public void close() throws IOException {
        if (this.socket != null) {
            this.socket.close();
        }
    }
}
