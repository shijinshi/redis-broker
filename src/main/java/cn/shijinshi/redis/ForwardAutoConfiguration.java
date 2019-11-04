package cn.shijinshi.redis;

import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.common.log.Access;
import cn.shijinshi.redis.common.log.IndexLogger;
import cn.shijinshi.redis.common.registry.RegistryService;
import cn.shijinshi.redis.common.registry.ZookeeperRegistryService;
import cn.shijinshi.redis.control.broker.Broker;
import cn.shijinshi.redis.control.broker.FixedForwardLauncher;
import cn.shijinshi.redis.control.broker.Launcher;
import cn.shijinshi.redis.control.controller.ReplicationController;
import cn.shijinshi.redis.control.rpc.RpcHelper;
import cn.shijinshi.redis.forward.Handler;
import cn.shijinshi.redis.forward.client.AutoRedisConnector;
import cn.shijinshi.redis.forward.handler.AppendHandler;
import cn.shijinshi.redis.forward.handler.ConnectionHandler;
import cn.shijinshi.redis.forward.handler.SupportHandler;
import cn.shijinshi.redis.forward.support.CommandSupports;
import cn.shijinshi.redis.sync.Appender;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Gui Jiahai
 */
@Configuration
@ConditionalOnProperty(value = "mode", havingValue = "onlyForward")
public class ForwardAutoConfiguration {

    @Bean
    public IndexLogger indexLogger(BrokerProperties properties) {
        return new IndexLogger(properties.getAppender().getLog(), Access.W);
    }

    @Bean
    public Launcher forwardLauncher(BrokerProperties properties, Broker broker, Appender appender, CommandSupports commandSupports) {
        return getLauncher(properties, broker, appender, commandSupports);
    }

    public static FixedForwardLauncher getLauncher(BrokerProperties properties, Broker broker, Appender appender, CommandSupports commandSupports) {
        AutoRedisConnector connector = new AutoRedisConnector();
        broker.addListener(connector);

        ConnectionHandler connectionHandler = new ConnectionHandler(connector, broker);
        AppendHandler appendHandler = new AppendHandler(connectionHandler, appender);
        Handler handler = new SupportHandler(appendHandler, broker, commandSupports);

        return new FixedForwardLauncher(properties, handler);
    }

    @Bean(destroyMethod = "close")
    public RegistryService registryService(BrokerProperties properties) {
        return new ZookeeperRegistryService(properties.getZookeeper().getAddress());
    }

    @Bean(initMethod = "init", destroyMethod = "destroy")
    public ReplicationController controller(RegistryService registryService, BrokerProperties properties, RpcHelper rpcHelper) {
        return new ReplicationController(registryService, properties, rpcHelper);
    }

    @Bean(initMethod = "init", destroyMethod = "destroy")
    public Broker broker(RegistryService registryService, BrokerProperties properties) {
        return new Broker(registryService, properties);
    }

}
