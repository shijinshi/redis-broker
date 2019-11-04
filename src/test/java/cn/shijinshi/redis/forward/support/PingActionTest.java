package cn.shijinshi.redis.forward.support;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author Gui Jiahai
 */
public class PingActionTest {

    private PingAction action = new PingAction();

    @Test
    public void test() throws ExecutionException, InterruptedException {

        CompletableFuture<byte[]> future = new CompletableFuture<>();
        Boolean result = action.apply(null, future);

        Assert.assertTrue(result);
        Assert.assertArrayEquals(future.get(), "+PONG\r\n".getBytes());

    }


}
