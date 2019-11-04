package cn.shijinshi.redis;

import cn.shijinshi.redis.common.log.Access;
import cn.shijinshi.redis.common.log.IndexLogger;
import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.common.registry.RegistryService;
import cn.shijinshi.redis.common.registry.ZookeeperRegistryService;
import cn.shijinshi.redis.control.broker.Broker;
import cn.shijinshi.redis.control.broker.FixedSyncLauncher;
import cn.shijinshi.redis.control.broker.Launcher;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Gui Jiahai
 */
@Configuration
@ConditionalOnProperty(value = "mode", havingValue = "onlySync")
public class SyncAutoConfiguration {

    @Bean
    public IndexLogger indexLogger(BrokerProperties properties) {
        return new IndexLogger(properties.getAppender().getLog(), Access.R);
    }

    @Bean
    public Launcher syncLauncher(IndexLogger indexLogger, BrokerProperties properties) {
        return new FixedSyncLauncher(indexLogger, properties);
    }

    @Bean(destroyMethod = "close")
    public RegistryService registryService(BrokerProperties properties) {
        return new ZookeeperRegistryService(properties.getZookeeper().getAddress());
    }

    @Bean(initMethod = "init", destroyMethod = "destroy")
    public Broker broker(RegistryService registryService, BrokerProperties properties) {
        return new Broker(registryService, properties);
    }

}
