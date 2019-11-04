package cn.shijinshi.redis.control.controller;

import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.common.param.HostAndPort;
import cn.shijinshi.redis.common.registry.ChildListener;
import cn.shijinshi.redis.common.registry.RegistryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 监听注册中心，实时获取Brokers列表，
 * 判断自己能否当选为Controller。
 *
 * @author Gui Jiahai
 */
public class BrokerDetector implements ChildListener {

    private static final Logger logger = LoggerFactory.getLogger(BrokerDetector.class);

    private final RegistryService registryService;
    private final BrokerProperties properties;
    private final BrokerDetectorCallback brokerDetectorCallback;
    private final HostAndPort localAddress;

    public BrokerDetector(RegistryService registryService, BrokerProperties properties, BrokerDetectorCallback brokerDetectorCallback) {
        this.registryService = Objects.requireNonNull(registryService);
        this.properties = Objects.requireNonNull(properties);
        this.brokerDetectorCallback = Objects.requireNonNull(brokerDetectorCallback);
        this.localAddress = HostAndPort.create(properties.getAddress(), properties.getPort());

        this.registryService.addChildListener(properties.getBrokersPath(), this);
    }

    /**
     * Broker注册到注册中心时，都带有序号，这个序号由注册中心保证递增唯一的。
     * 当注册中心的Brokers列表发生变化时，BrokerDetector收到最新的Broker列表。
     * 获取序号，然后序号最小的当选为Controller
     */
    @Override
    public void childChanged(String path, List<String> children) {
        if (path == null || !path.equals(properties.getBrokersPath()) || children == null || children.isEmpty()) {
            return;
        }

        TreeMap<Long, HostAndPort> addressMap = new TreeMap<>();
        for (String child : children) {
            if (child != null && !child.isEmpty()) {
                String[] subs = child.split("-");
                if (subs.length == 2) {
                    try {
                        HostAndPort address = HostAndPort.create(subs[0]);
                        Long seq = Long.parseLong(subs[1]);
                        addressMap.put(seq, address);
                        continue;
                    } catch (RuntimeException ignored) {
                    }
                }
                logger.warn("Cannot parse child [{}] to HostAndPort, ignore it", child);
            }
        }

        if (addressMap.isEmpty()) {
            logger.error("Cannot get any valid address with children in path: {}", path);
            return;
        }

        boolean active = false;
        Map.Entry<Long, HostAndPort> first = addressMap.firstEntry();
        /*
        判断当前节点是否是序号最小的
         */
        if (localAddress.equals(first.getValue())) {
            active = true;
        }

        Set<HostAndPort> addressSet = new HashSet<>(addressMap.values());
        this.notify(active, first.getKey(), addressSet);
    }

    private void notify(boolean active, long epoch, Set<HostAndPort> addressSet)  {
        // 详细解释请参考 BrokerDetectorCallback
        if (!active) {
            epoch = -1;
            addressSet = null;
        }
        this.brokerDetectorCallback.brokerChanged(active, epoch, addressSet);
    }

    public void close() {
        this.registryService.removeChildListener(properties.getBrokersPath(), this);
    }

}
