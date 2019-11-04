package cn.shijinshi.redis.control.controller;

import cn.shijinshi.redis.common.param.HostAndPort;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Gui Jiahai
 */
public class RedisStandaloneDetectorTest {

    private RedisStandaloneDetector detector;

    @Before
    public void setup() {
        detector = new RedisStandaloneDetector(HostAndPort.create("192.168.10.56", 6279), System.out::println);
    }

    @Test
    public void test() {
        detector.start();
        detector.stop();
        detector.start();
        detector.stop();
    }

}
