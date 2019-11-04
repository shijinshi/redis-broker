package cn.shijinshi.redis.common.adapt;

import cn.shijinshi.redis.common.prop.RedisProperties;
import cn.shijinshi.redis.common.param.HostAndPort;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Gui Jiahai
 */
public class ReplicatorAdaptorFactory {

    public static ReplicatorAdaptor create(RedisProperties redis) {
        if (redis.getHostAndPort() != null) {
            RedisClient client = RedisClient.create("redis://" + redis.getHostAndPort().format());
            StatefulRedisConnection<byte[], byte[]> connect = client.connect(ByteArrayCodec.INSTANCE);
            RedisAsyncCommands<byte[], byte[]> async = connect.async();

            ReplicatorHandler handler = new LettuceHandler(async, () -> {
                connect.close();
                client.shutdown();
            });
            return new ReplicatorAdaptor(handler);

        } else if (redis.getNodes() != null && !redis.getNodes().isEmpty()) {

            List<RedisURI> uris = new ArrayList<>();
            for (HostAndPort node : redis.getNodes()) {
                uris.add(RedisURI.builder().withHost(node.getHost()).withPort(node.getPort()).build());
            }
            RedisClusterClient client = RedisClusterClient.create(uris);
            StatefulRedisClusterConnection<byte[], byte[]> connect = client.connect(ByteArrayCodec.INSTANCE);
            RedisAdvancedClusterAsyncCommands<byte[], byte[]> async = connect.async();

            ReplicatorHandler handler = new LettuceHandler(async, () -> {
                connect.close();
                client.shutdown();
            });
            return new ReplicatorAdaptor(handler);

        } else {
            throw new IllegalArgumentException("Cannot get any redis address");
        }
    }

}
