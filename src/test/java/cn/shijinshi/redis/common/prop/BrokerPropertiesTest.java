package cn.shijinshi.redis.common.prop;

import org.junit.Assert;
import org.junit.Test;

import java.net.UnknownHostException;

/**
 * @author Gui Jiahai
 */
public class BrokerPropertiesTest {

    @Test
    public void test() throws UnknownHostException {

        BrokerProperties properties = new BrokerProperties();
        properties.setPort(6479);
        properties.getZookeeper().setRoot("redis");
        properties.getAppender().getLog().setDir("/tmp");

        properties.init();

        Assert.assertNotNull(properties.getAddress());
        Assert.assertEquals(properties.getReplicationPath(), "/redis/replication");
        Assert.assertEquals(properties.getBrokersPath(), "/redis/brokers");

        Assert.assertEquals(properties.getAppender().getLog().getDir(), "/tmp/6479");

    }

}
