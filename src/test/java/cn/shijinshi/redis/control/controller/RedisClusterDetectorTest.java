package cn.shijinshi.redis.control.controller;

import cn.shijinshi.redis.common.param.HostAndPort;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * @author Gui Jiahai
 */
public class RedisClusterDetectorTest {

    private RedisClusterDetector detector;

    @Before
    public void setup() {
        List<HostAndPort> nodes = Collections.singletonList(HostAndPort.create("192.168.1.105:7003"));
        detector = new RedisClusterDetector(nodes, System.out::println);
    }

    @Test
    public void test() {
        detector.start();
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(5));
        detector.stop();
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(5));
        detector.start();
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(5));
    }

    @After
    public void after() {
        detector.stop();
    }

}
