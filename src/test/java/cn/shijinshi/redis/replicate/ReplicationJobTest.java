package cn.shijinshi.redis.replicate;

import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.common.prop.RedisProperties;
import org.junit.Test;

import java.util.Collections;

/**
 * @author Gui Jiahai
 */
public class ReplicationJobTest {

    private ReplicationJob job;

    @Test
    public void test_standalone() throws InterruptedException {

        RedisProperties source = new RedisProperties();
        source.setHostAndPort(HostAndPort.create("192.168.100.101", 6379));

        RedisProperties target = new RedisProperties();
        target.setHostAndPort(HostAndPort.create("192.168.100.101", 6479));

        startJob(source, target);
        job.start();

        job.join();
    }

    @Test
    public void test_cluster() throws InterruptedException {
        RedisProperties source = new RedisProperties();
        source.setNodes(Collections.singletonList(HostAndPort.create("192.168.1.105", 7001)));

        RedisProperties target = new RedisProperties();
        target.setHostAndPort(HostAndPort.create("192.168.100.101", 6479));

        startJob(source, target);
        job.start();

        job.join();
    }

    private void startJob(RedisProperties source, RedisProperties target) {
        job = new ReplicationJob(source, target, new Observer<ReplicationJob>() {
            @Override
            public void onNext(ReplicationJob obj) {
                System.out.println("onNext");

                if (job != null) {
                    job.completeWithTick("key-tick".getBytes());
                }
            }

            @Override
            public void onCompleted(ReplicationJob obj) {
                System.out.println("onCompleted");
            }

            @Override
            public void onError(ReplicationJob obj) {
                System.out.println("onError");
            }
        });
    }


}
