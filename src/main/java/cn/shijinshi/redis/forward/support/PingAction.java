package cn.shijinshi.redis.forward.support;

import cn.shijinshi.redis.common.protocol.RedisRequest;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * @author Gui Jiahai
 */
@Component("ping")
@Lazy
public class PingAction implements Action {

    private final byte[] reply = "+PONG\r\n".getBytes();

    @Override
    public Boolean apply(RedisRequest redisRequest, CompletableFuture<byte[]> future) {
        future.complete(reply);
        return Boolean.TRUE;
    }
}
