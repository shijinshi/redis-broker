package cn.shijinshi.redis.sync;

import cn.shijinshi.redis.common.protocol.RedisRequest;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author Gui Jiahai
 */
public interface Appender extends Closeable {

    void append(RedisRequest request) throws IOException;

}
