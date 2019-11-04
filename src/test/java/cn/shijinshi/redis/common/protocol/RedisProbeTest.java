package cn.shijinshi.redis.common.protocol;

import cn.shijinshi.redis.common.param.Cluster;
import cn.shijinshi.redis.common.param.HostAndPort;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Gui Jiahai
 */
public class RedisProbeTest {

    @Test
    public void test() throws IOException {

        RedisProbe probe = new RedisProbe(HostAndPort.create("192.168.1.105", 7001));
        Cluster cluster = probe.discover();

        Assert.assertNotNull(cluster);
        Assert.assertNotNull(cluster.getNodes());
        Assert.assertFalse(cluster.getNodes().isEmpty());

        cluster = probe.discover();
        Assert.assertNotNull(cluster);
        Assert.assertNotNull(cluster.getNodes());
        Assert.assertFalse(cluster.getNodes().isEmpty());

        probe.close();

    }
}
