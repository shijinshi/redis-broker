package cn.shijinshi.redis.control.controller;

import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.common.registry.RegistryException;
import cn.shijinshi.redis.common.registry.RegistryService;
import cn.shijinshi.redis.common.registry.ZookeeperRegistryService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.mockito.Mockito.when;

/**
 * @author Gui Jiahai
 */
@RunWith(MockitoJUnitRunner.class)
public class BrokerDetectorTest {

    private BrokerDetector detector;

    private RegistryService registryService;

    @Mock
    private BrokerProperties properties;

    @Before
    public void setup() throws RegistryException {
        registryService = new ZookeeperRegistryService("192.168.100.101:2181");

        when(properties.getBrokersPath()).thenReturn("/redis_test/brokers");
        when(properties.getAddress()).thenReturn("192.168.10.56");
        when(properties.getPort()).thenReturn(6279);

        if (!registryService.checkExists(properties.getBrokersPath())) {
            registryService.create(properties.getBrokersPath(), false, false);
        }

        detector = new BrokerDetector(registryService, properties, (addressSet, active, epoch) -> {
            System.out.println();
            System.out.println(String.format("active: %s, epoch: %s", active, epoch));
            System.out.println(addressSet);
            System.out.println();
        });
    }

    @Test
    public void test() throws RegistryException {
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
        String s1 = registryService.create(properties.getBrokersPath() + "/192.168.10.56:6001-", true, true);
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
        registryService.create(properties.getBrokersPath() + "/192.168.10.56:6279-", true, true);
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
        registryService.create(properties.getBrokersPath() + "/192.168.10.56:6002-", true, true);
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
        registryService.delete(s1);

        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
    }

    @After
    public void after() {
        registryService.close();
    }

}
