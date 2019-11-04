package cn.shijinshi.redis;

import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.common.registry.EmptyRegistryService;
import cn.shijinshi.redis.common.registry.RegistryService;
import cn.shijinshi.redis.control.broker.Broker;
import cn.shijinshi.redis.control.broker.FixedReplLauncher;
import cn.shijinshi.redis.control.broker.Launcher;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Gui Jiahai
 */
@Configuration
@ConditionalOnProperty(value = "mode", havingValue = "onlyRepl")
public class ReplAutoConfiguration {

    @Bean
    public Launcher replLauncher(BrokerProperties properties) {
        return new FixedReplLauncher(properties);
    }

    @Bean(destroyMethod = "close")
    public RegistryService registryService() {
        return new EmptyRegistryService();
    }

    @Bean(initMethod = "init", destroyMethod = "destroy")
    public Broker broker(RegistryService registryService, BrokerProperties properties) {
        return new Broker(registryService, properties);
    }

}
