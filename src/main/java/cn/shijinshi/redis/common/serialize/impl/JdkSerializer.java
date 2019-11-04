package cn.shijinshi.redis.common.serialize.impl;

import cn.shijinshi.redis.common.serialize.Serializer;

import java.io.*;

/**
 * @author Gui Jiahai
 */
public class JdkSerializer implements Serializer {

    @Override
    public byte[] serialize(Object object) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (ObjectOutputStream stream = new ObjectOutputStream(output)) {
            stream.writeObject(object);
            stream.flush();
            return output.toByteArray();
        }
    }

    @Override
    public <T> T deserialize(byte[] data, Class<T> clazz) throws IOException {
        try (ObjectInputStream stream = new ObjectInputStream(new ByteArrayInputStream(data))) {
            return clazz.cast(stream.readObject());
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

}
