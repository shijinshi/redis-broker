package cn.shijinshi.redis.forward.support;

import cn.shijinshi.redis.common.protocol.RedisRequest;

import java.util.concurrent.CompletableFuture;

/**
 * @author Gui Jiahai
 */
public interface Action {

    /**
     * 预处理RedisRequest
     * @return 如果能成功处理，则返回TRUE，否则返回FALSE
     */
    Boolean apply(RedisRequest redisRequest, CompletableFuture<byte[]> completableFuture);
}
