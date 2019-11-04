package cn.shijinshi.redis.forward.support;

import cn.shijinshi.redis.common.Constants;
import cn.shijinshi.redis.common.protocol.RedisRequest;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * @author Gui Jiahai
 */
@Component("quit")
@Lazy
public class QuitAction implements Action {

    @Override
    public Boolean apply(RedisRequest redisRequest, CompletableFuture<byte[]> future) {
        future.complete(Constants.OK_REPLY);
        return Boolean.TRUE;
    }
}
