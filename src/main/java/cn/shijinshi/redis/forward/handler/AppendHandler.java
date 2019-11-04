package cn.shijinshi.redis.forward.handler;

import cn.shijinshi.redis.common.error.ErrorHandler;
import cn.shijinshi.redis.common.protocol.RedisRequest;
import cn.shijinshi.redis.forward.Handler;
import cn.shijinshi.redis.forward.support.Support;
import cn.shijinshi.redis.sync.Appender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * 将请求报文写入到队列中（磁盘）
 *
 * @author Gui Jiahai
 */
public class AppendHandler implements Handler {
    private static final Logger logger = LoggerFactory.getLogger(AppendHandler.class);

    private final Handler next;
    private final Appender appender;

    public AppendHandler(Handler next, Appender appender) {
        this.next = Objects.requireNonNull(next);
        this.appender = Objects.requireNonNull(appender);
    }

    @Override
    public void handle(RedisRequest request, Support support, CompletableFuture<byte[]> future) {
        future.thenAccept(bytes -> {
            /*
        在redis通信协议中，减号表示错误信息
        如果没有发生错误，则表示命令生效，应该将命令备份
         */
            if (support.isBackup() && bytes[0] != '-') {
                try {
                    appender.append(request);
                } catch (IOException e) {
                    logger.error("Failed to append request", e);
                    ErrorHandler.handle(e);
                }
            }
        });

        this.next.handle(request, support, future);
    }

    @Override
    public void close() {
        try {
            this.next.close();
        } catch (IOException ignored) {
        }
        try {
            this.appender.close();
        } catch (IOException ignored) {}
    }
}
