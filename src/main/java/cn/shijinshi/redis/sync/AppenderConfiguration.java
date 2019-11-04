package cn.shijinshi.redis.sync;

import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.common.log.IndexLogger;
import cn.shijinshi.redis.sync.appender.AsyncAppender;
import cn.shijinshi.redis.sync.appender.NoAppender;
import cn.shijinshi.redis.sync.appender.SyncAppender;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import static cn.shijinshi.redis.common.prop.AppenderProperties.*;

@Configuration
public class AppenderConfiguration {

    @Bean(destroyMethod = "")
    @Lazy
    public Appender create(IndexLogger indexLogger, BrokerProperties properties) {
        String appenderType = properties.getAppender().getType();
        if (SYNC_TYPE.equalsIgnoreCase(appenderType)) {
            return new SyncAppender(indexLogger);
        } else if (ASYNC_TYPE.equalsIgnoreCase(appenderType)) {
            return new AsyncAppender(indexLogger);
        } else if (NO_TYPE.equalsIgnoreCase(appenderType)) {
            return new NoAppender(indexLogger);
        } else {
            throw new IllegalStateException("Unrecognized appender type, must be no, async or sync");
        }
    }

}
