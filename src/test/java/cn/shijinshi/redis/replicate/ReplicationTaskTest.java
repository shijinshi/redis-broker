package cn.shijinshi.redis.replicate;

import cn.shijinshi.redis.common.adapt.LettuceHandler;
import cn.shijinshi.redis.common.adapt.ReplicatorAdaptor;
import cn.shijinshi.redis.common.adapt.ReplicatorHandler;
import cn.shijinshi.redis.common.param.HostAndPort;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Gui Jiahai
 */
public class ReplicationTaskTest {

    private ReplicationTask task;

    @Before
    public void setup() {

        RedisClient client = RedisClient.create("redis://192.168.100.101:6479");
        StatefulRedisConnection<byte[], byte[]> connect = client.connect(ByteArrayCodec.INSTANCE);
        RedisAsyncCommands<byte[], byte[]> async = connect.async();

        ReplicatorHandler handler = new LettuceHandler(async, () -> {
            connect.close();
            client.shutdown();
        });

        ReplicatorAdaptor adaptor = new ReplicatorAdaptor(handler);

        task = new ReplicationTask(HostAndPort.create("192.168.100.101", 6379), adaptor, new Observer<HostAndPort>() {
            @Override
            public void onNext(HostAndPort obj) {
                System.out.println("onNext: " + obj);
                task.completeWithTick("key-tick".getBytes());
            }

            @Override
            public void onCompleted(HostAndPort obj) {
                System.out.println("onCompleted: " + obj);
            }

            @Override
            public void onError(HostAndPort obj) {
                System.out.println("onError: " + obj);
            }
        });
    }

    @Test
    public void test() throws InterruptedException {
        task.start();
        task.join();
    }

}
