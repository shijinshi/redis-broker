package cn.shijinshi.redis.common.serialize.impl;

import cn.shijinshi.redis.common.serialize.Serializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * @author Gui Jiahai
 */
public class JacksonSerializer implements Serializer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(Object object) throws IOException {
        return objectMapper.writeValueAsBytes(object);
    }

    @Override
    public <T> T deserialize(byte[] data, Class<T> clazz) throws IOException {
        return objectMapper.readValue(data, clazz);
    }
}
