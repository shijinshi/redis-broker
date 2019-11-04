package cn.shijinshi.redis.sync.appender;

import cn.shijinshi.redis.common.Shutdown;
import cn.shijinshi.redis.common.error.ErrorHandler;
import cn.shijinshi.redis.common.log.IndexLogger;
import cn.shijinshi.redis.common.protocol.RedisRequest;
import cn.shijinshi.redis.sync.Appender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * 异步的持久化
 * 性能好，但是有丢失数据的风险
 *
 * @author Gui Jiahai
 */
public class AsyncAppender implements Appender, Runnable {

    private static final Logger logger = LoggerFactory.getLogger(AsyncAppender.class);

    private final IndexLogger indexLogger;
    private final BlockingQueue<RedisRequest> queue = new LinkedBlockingDeque<>(1024 * 10);

    private volatile boolean closed = false;

    public AsyncAppender(IndexLogger indexLogger) {
        this.indexLogger = Objects.requireNonNull(indexLogger);

        Thread thread = new Thread(this, "async-appender-thread");
        thread.setDaemon(true);
        thread.start();
        Shutdown.addThread(thread);
    }

    @Override
    public void append(RedisRequest request) throws IOException {
        if (closed) {
            throw new IOException("output stream closed");
        }
        queue.add(request);
    }

    public void run() {
        try {
            OutputStream output = indexLogger.getOutputStream();
            while (!closed) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1500));
                consumeQueue(output);
            }
            consumeQueue(output);
        } finally {
            try {
                this.indexLogger.getOutputStream().close();
            } catch (IOException ignored) {}
        }
    }

    private void consumeQueue(OutputStream output) {
        RedisRequest req;
        while ((req = queue.poll()) != null) {
            try {
                output.write(req.getContent());
            } catch (IOException e) {
                logger.error("Failed to write request to aof file", e);
                ErrorHandler.handle(e);
            }
        }
        try {
            output.flush();
        } catch (IOException e) {
            logger.error("Failed to flush request to aof file", e);
            ErrorHandler.handle(e);
        }

    }

    @Override
    public void close() {
        closed = true;
    }
}
