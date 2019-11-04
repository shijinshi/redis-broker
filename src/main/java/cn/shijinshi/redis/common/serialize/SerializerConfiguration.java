package cn.shijinshi.redis.common.serialize;

import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.common.serialize.impl.JacksonSerializer;
import cn.shijinshi.redis.common.serialize.impl.JdkSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

/**
 * @author Gui Jiahai
 */
@Configuration
public class SerializerConfiguration {

    private static final String JACKSON_TYPE = "jackson";
    private static final String JDK_TYPE = "jdk";

    @Bean
    @Lazy
    public Serializer serializer(BrokerProperties properties) {
        if (properties.getSerialize() == null || properties.getSerialize().trim().isEmpty()
                || properties.getSerialize().equalsIgnoreCase(JACKSON_TYPE)) {
            return new JacksonSerializer();
        } else if (JDK_TYPE.equalsIgnoreCase(properties.getSerialize())) {
            return new JdkSerializer();
        } else {
            throw new IllegalArgumentException("Cannot process property of serialize: " + properties.getSerialize());
        }
    }

}
