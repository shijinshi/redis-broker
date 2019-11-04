package cn.shijinshi.redis.forward.support;

import cn.shijinshi.redis.common.protocol.RedisRequest;
import cn.shijinshi.redis.control.broker.Broker;
import cn.shijinshi.redis.control.rpc.Answer;
import cn.shijinshi.redis.control.rpc.Indication;
import cn.shijinshi.redis.control.rpc.RpcHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * 用于Controller和Brokers进行通信
 *
 * @author Gui Jiahai
 */
@Component("rpc")
@Lazy
public class RpcAction implements Action {

    private static final Logger logger = LoggerFactory.getLogger(RpcAction.class);

    private final byte[] errReply;

    private final Broker broker;
    private final RpcHelper rpcHelper;

    public RpcAction(Broker broker, RpcHelper rpcHelper) {
        this.broker = broker;
        this.rpcHelper = rpcHelper;

        try {
            Answer answer = Answer.bad("Failed to parse bytes to Indication or Answer");
            errReply = rpcHelper.toResponse(answer);

        } catch (IOException e) {
            logger.error("Failed to serialize Answer.bad() to bytes", e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Boolean apply(RedisRequest redisRequest, CompletableFuture<byte[]> future) {
        try {
            Indication indication = rpcHelper.fromRequest(redisRequest);
            Answer answer = broker.answer(indication);
            byte[] bytes = rpcHelper.toResponse(answer);
            future.complete(bytes);
        } catch (IOException e) {
            future.complete(errReply);
        }
        return Boolean.TRUE;
    }

}
