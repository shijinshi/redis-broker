package cn.shijinshi.redis.forward;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * Redis连接器
 *
 * 实现类需要将请求异步发送给Redis节点，并返回处理结果
 *
 * @author Gui Jiahai
 */
public interface RedisConnector extends Closeable {

    void send(Object request, CompletableFuture<byte[]> future);

    boolean isClosed();

}
