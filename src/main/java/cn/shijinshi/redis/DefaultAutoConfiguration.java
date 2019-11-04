package cn.shijinshi.redis;

import cn.shijinshi.redis.common.log.Access;
import cn.shijinshi.redis.common.log.IndexLogger;
import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.common.registry.RegistryService;
import cn.shijinshi.redis.common.registry.ZookeeperRegistryService;
import cn.shijinshi.redis.control.broker.*;
import cn.shijinshi.redis.control.controller.ReplicationController;
import cn.shijinshi.redis.control.rpc.RpcHelper;
import cn.shijinshi.redis.forward.support.CommandSupports;
import cn.shijinshi.redis.sync.Appender;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

/**
 * @author Gui Jiahai
 */
@Configuration
@ConditionalOnMissingBean({ReplAutoConfiguration.class, SyncAutoConfiguration.class, ForwardAutoConfiguration.class})
public class DefaultAutoConfiguration {

    @Bean(initMethod = "init", destroyMethod = "destroy", name = "broker")
    public Broker broker(RegistryService registryService, BrokerProperties properties) {
        return new Broker(registryService, properties);
    }

    @Bean(destroyMethod = "")
    public IndexLogger indexLogger(BrokerProperties properties) {
        return new IndexLogger(properties.getAppender().getLog(), Access.RW);
    }

    @Bean
    public FixedForwardLauncher forwardLauncher(BrokerProperties properties, Broker broker, Appender appender, CommandSupports commandSupports) {
        return ForwardAutoConfiguration.getLauncher(properties, broker, appender, commandSupports);
    }

    @Bean
    public RelatedSyncLauncher syncLauncher(IndexLogger indexLogger, BrokerProperties properties) {
        return new RelatedSyncLauncher(indexLogger, properties);
    }

    @Bean
    public RelatedReplLauncher replLauncher(BrokerProperties properties) {
        return new RelatedReplLauncher(properties);
    }

    @Bean(destroyMethod = "close")
    public RegistryService registryService(BrokerProperties properties) {
        return new ZookeeperRegistryService(properties.getZookeeper().getAddress());
    }

    @Bean(initMethod = "init", destroyMethod = "destroy")
    @DependsOn("broker")
    public ReplicationController controller(RegistryService registryService, BrokerProperties properties, RpcHelper rpcHelper) {
        return new ReplicationController(registryService, properties, rpcHelper);
    }

}
