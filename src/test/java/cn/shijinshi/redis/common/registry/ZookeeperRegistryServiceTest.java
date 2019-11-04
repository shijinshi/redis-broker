package cn.shijinshi.redis.common.registry;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * @author Gui Jiahai
 */
public class ZookeeperRegistryServiceTest {

    private RegistryService registryService;
    private final String root = "/redis_test";
    private final String path = root + "/my";

    @Before
    public void setup() throws RegistryException {
        registryService = new ZookeeperRegistryService("192.168.100.101:2181");
        if (!registryService.checkExists(root)) {
            String s = registryService.create(root, false, false);
            Assert.assertEquals(s, root);
        }
    }

    @Test
    public void test_path() throws RegistryException {
        String s = registryService.create(path, false, false);
        Assert.assertTrue(registryService.checkExists(s));
        Assert.assertEquals(s, path);

        registryService.delete(path);
        Assert.assertFalse(registryService.checkExists(path));
    }

    @Test
    public void test_getChildren() throws RegistryException {
        registryService.create(path, false, false);
        registryService.addChildListener(path, (path, children) -> System.out.println(children));

        try {
            registryService.create(path + "/a", true, false);
            registryService.create(path + "/b", true, false);

            List<String> children = registryService.getChildren(path);
            Assert.assertEquals(new HashSet<>(children), new HashSet<>(Arrays.asList("a", "b")));

        } finally {
            registryService.delete(path + "/a");
            registryService.delete(path + "/b");
            registryService.delete(path);
        }
    }

    @Test
    public void test_data() throws RegistryException {
        registryService.create(path, false, false);
        registryService.addDataListener(path, (path, data) -> System.out.println(new String(data)));

        try {
            byte[] data = "HelloWorld".getBytes();
            registryService.setData(path, data);

            byte[] bytes = registryService.getData(path);
            Assert.assertArrayEquals(data, bytes);

        } finally {
            registryService.delete(path);
        }
    }

    @After
    public void after() {
        if (registryService != null) {
            registryService.close();
        }
    }

}
