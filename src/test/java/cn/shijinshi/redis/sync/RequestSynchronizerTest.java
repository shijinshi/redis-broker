package cn.shijinshi.redis.sync;

import cn.shijinshi.redis.common.log.IndexLogger;
import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.common.prop.RedisProperties;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.when;

/**
 * @author Gui Jiahai
 */
@RunWith(MockitoJUnitRunner.class)
public class RequestSynchronizerTest {

    private RequestSynchronizer synchronizer;

    @Mock
    private IndexLogger logger;

    @Before
    public void setup() {
        InputStream input = RequestSynchronizerTest.class.getClassLoader().getResourceAsStream("appendonly.aof");
        when(logger.getInputStream()).thenReturn(input);

        RedisProperties target = new RedisProperties();
        target.setHostAndPort(HostAndPort.create("192.168.100.101", 6479));

        synchronizer = new RequestSynchronizer(logger, target);
    }

    @Test
    public void test() throws InterruptedException {
        synchronizer.start();
        synchronizer.stop(12, TimeUnit.SECONDS);
        synchronizer.join();
    }

}
