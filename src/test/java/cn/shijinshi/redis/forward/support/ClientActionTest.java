package cn.shijinshi.redis.forward.support;

import cn.shijinshi.redis.common.protocol.RedisCodec;
import cn.shijinshi.redis.common.protocol.RedisRequest;
import cn.shijinshi.redis.common.util.UnsafeByteString;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author Gui Jiahai
 */
public class ClientActionTest {

    private ClientAction action = new ClientAction();

    @Test
    public void test_client_list() throws ExecutionException, InterruptedException, IOException {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        RedisRequest request = clientRequest("list");

        Boolean result = action.apply(request, future);
        Assert.assertEquals(result, Boolean.TRUE);

        byte[] bytes = future.get();
        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        byte[] reply = RedisCodec.decodeReply(input);
        Assert.assertNotNull(reply);
    }

    @Test
    public void test_client_setname() throws ExecutionException, InterruptedException, IOException {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        RedisRequest request = clientRequest("setname");

        Boolean result = action.apply(request, future);
        Assert.assertEquals(result, Boolean.TRUE);

        byte[] bytes = future.get();
        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        byte[] reply = RedisCodec.decodeReply(input);
        Assert.assertArrayEquals(reply, "+OK\r\n".getBytes());
    }

    @Test
    public void test_client_other() {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        RedisRequest request = clientRequest("other");

        Boolean result = action.apply(request, future);
        Assert.assertEquals(result, Boolean.FALSE);
        Assert.assertFalse(future.isDone());
    }

    private RedisRequest clientRequest(String subCommand) {
        return new RedisRequest(null, new UnsafeByteString("client"), new UnsafeByteString(subCommand));
    }

}
