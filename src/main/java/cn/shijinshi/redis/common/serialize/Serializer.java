package cn.shijinshi.redis.common.serialize;

import java.io.IOException;

/**
 * 序列化接口
 *
 * @author Gui Jiahai
 */
public interface Serializer {

    byte[] serialize(Object object) throws IOException;

    <T> T deserialize(byte[] data, Class<T> clazz) throws IOException;

}
