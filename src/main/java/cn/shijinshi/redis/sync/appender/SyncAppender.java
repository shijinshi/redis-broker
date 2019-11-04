package cn.shijinshi.redis.sync.appender;

import cn.shijinshi.redis.common.log.IndexLogger;
import cn.shijinshi.redis.common.protocol.RedisRequest;
import cn.shijinshi.redis.sync.Appender;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

/**
 * 同步的持久化
 * 可靠性强，但是性能差
 *
 * @author Gui Jiahai
 */
public class SyncAppender implements Appender {

    private final IndexLogger indexLogger;
    private boolean closed = false;

    public SyncAppender(IndexLogger indexLogger) {
        this.indexLogger = Objects.requireNonNull(indexLogger);
    }

    @Override
    public synchronized void append(RedisRequest request) throws IOException {
        if (closed) {
            throw new IOException("output stream closed");
        }
        OutputStream output = indexLogger.getOutputStream();
        output.write(request.getContent());
        output.flush();
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        try {
            this.indexLogger.getOutputStream().close();
        } catch (IOException ignored) {}
    }
}
