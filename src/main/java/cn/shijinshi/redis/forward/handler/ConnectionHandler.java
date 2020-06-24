package cn.shijinshi.redis.forward.handler;

import cn.shijinshi.redis.common.Constants;
import cn.shijinshi.redis.common.param.Cluster;
import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.common.param.Node;
import cn.shijinshi.redis.common.param.Range;
import cn.shijinshi.redis.common.protocol.RedisRequest;
import cn.shijinshi.redis.control.broker.Broker;
import cn.shijinshi.redis.forward.Handler;
import cn.shijinshi.redis.forward.RedisConnector;
import cn.shijinshi.redis.forward.support.Support;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * 将请求报文写入到Redis节点中
 *
 * @author Gui Jiahai
 */
public class ConnectionHandler implements Handler {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionHandler.class);

    private static final Unsafe UNSAFE;
    private static final long RESULT_OFFSET;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (Unsafe) field.get(null);
            RESULT_OFFSET = UNSAFE.objectFieldOffset(CompletableFuture.class.getDeclaredField("result"));
        } catch (Throwable t) {
            throw new Error(t);
        }
    }

    private final RedisConnector redisConnector;
    private final Broker broker;

    public ConnectionHandler(RedisConnector redisConnector, Broker broker) {
        this.redisConnector = Objects.requireNonNull(redisConnector);
        this.broker = Objects.requireNonNull(broker);
    }

    @Override
    public void handle(RedisRequest request, Support support, CompletableFuture<byte[]> future) {
        future.thenAccept(bytes -> {
            if (bytes[0] == '-') {
                byte[] newBytes = convert(bytes);
                if (newBytes != null) {
                    UNSAFE.putObjectVolatile(future, RESULT_OFFSET, newBytes);
//                    UNSAFE.compareAndSwapObject(future, FUTURE_RESULT, bytes, newBytes);
                }
            }
        });

        redisConnector.send(request.getContent(), future);
    }

    private byte[] convert(byte[] bytes) {
        if (shouldBeConverted(bytes)) {
            String reply = new String(bytes, 0, bytes.length);
            String[] strings = reply.split(" ");
            if (strings.length < 3) {
                return bytes;
            }
            int slot = Integer.parseInt(strings[1]);
            HostAndPort b = null;
            Cluster cluster = broker.getCluster();
            for (Node node : cluster.getNodes()) {
                for (Range range : node.getRanges()) {
                    if (range.getLowerBound() <= slot && slot <= range.getUpperBound()) {
                        b = node.getBroker();
                        break;
                    }
                }
            }
            if (b == null) {
                logger.error("Cannot convert slot to broker, reply from redis: {}", reply);
                return null;
            }

            String builder = strings[0] + ' ' +
                    strings[1] + ' ' +
                    b.format() + Constants.CRLF_STR;
            return builder.getBytes();
        }
        return bytes;
    }

    private boolean shouldBeConverted(byte[] bytes) {
        return bytes.length >= 7 && (bytes[1] == 'M' && bytes[2] == 'O' && bytes[3] == 'V' && bytes[4] == 'E' && bytes[5] == 'D' && bytes[6] == ' ')
                || (bytes[1] == 'A' && bytes[2] == 'S' && bytes[3] == 'K' && bytes[4] == ' ');
    }

    @Override
    public void close() {
        try {
            this.redisConnector.close();
        } catch (IOException ignored) {}
    }
}
