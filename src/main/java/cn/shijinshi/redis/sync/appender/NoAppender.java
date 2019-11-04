package cn.shijinshi.redis.sync.appender;

import cn.shijinshi.redis.common.log.IndexLogger;
import cn.shijinshi.redis.common.protocol.RedisRequest;
import cn.shijinshi.redis.sync.Appender;

import java.io.IOException;
import java.util.Objects;

/**
 * 丢弃RedisRequest
 * 如果不希望同步数据到target redis (cluster)，
 * 可以考虑这个方案
 *
 * @author Gui Jiahai
 */
public class NoAppender implements Appender {

    private final IndexLogger indexLogger;

    public NoAppender(IndexLogger indexLogger) {
        this.indexLogger = Objects.requireNonNull(indexLogger);
    }

    @Override
    public void append(RedisRequest request) {
    }

    @Override
    public void close() {
        try {
            this.indexLogger.getOutputStream().close();
        } catch (IOException ignored) {}
    }
}
