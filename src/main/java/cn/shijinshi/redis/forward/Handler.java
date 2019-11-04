package cn.shijinshi.redis.forward;

import cn.shijinshi.redis.common.protocol.RedisRequest;
import cn.shijinshi.redis.forward.support.Support;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * 在请求报文RedisRequest发送到Redis节点之前，
 * 可能会先进行预处理，比如，检查是否支持该命令。
 *
 * @author Gui Jiahai
 */
public interface Handler extends Closeable {

    void handle(RedisRequest request, Support support, CompletableFuture<byte[]> future);

}
