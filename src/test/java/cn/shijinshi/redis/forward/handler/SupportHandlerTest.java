package cn.shijinshi.redis.forward.handler;

import cn.shijinshi.redis.common.protocol.RedisRequest;
import cn.shijinshi.redis.common.util.UnsafeByteString;
import cn.shijinshi.redis.control.broker.Broker;
import cn.shijinshi.redis.forward.Handler;
import cn.shijinshi.redis.forward.support.Action;
import cn.shijinshi.redis.forward.support.CommandSupports;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

/**
 * @author Gui Jiahai
 */
@RunWith(MockitoJUnitRunner.class)
public class SupportHandlerTest {

    private SupportHandler handler;
    @Mock
    private Handler nextHandler;
    @Mock
    private Broker broker;

    @Before
    public void setup() {
        when(broker.isMaster()).thenReturn(Boolean.TRUE);

        Map<String, Action> actionMap = new HashMap<>();
        actionMap.put("get", (redisRequest, completableFuture) -> {
            completableFuture.complete("+GET_OK".getBytes());
            return Boolean.TRUE;
        });
        actionMap.put("set", (redisRequest, completableFuture) -> Boolean.FALSE);

        CommandSupports supports = new CommandSupports(actionMap);
        supports.init();
        handler = new SupportHandler(nextHandler, broker, supports);
    }

    @Test
    public void test_not_support() throws ExecutionException, InterruptedException {
        RedisRequest request = new RedisRequest(null, new UnsafeByteString("TEST"), null);

        CompletableFuture<byte[]> future = new CompletableFuture<>();
        handler.handle(request, null, future);
        Assert.assertArrayEquals(future.get(), "-ERR Unsupported command 'TEST'\r\n".getBytes());
    }

    @Test
    public void test_action_TRUE() throws ExecutionException, InterruptedException {
        RedisRequest request = new RedisRequest(null, new UnsafeByteString("get"), null);

        CompletableFuture<byte[]> future = new CompletableFuture<>();
        handler.handle(request, null, future);
        Assert.assertArrayEquals(future.get(), "+GET_OK".getBytes());
    }

    @Test
    public void test_action_FALSE() {
        RedisRequest request = new RedisRequest(null, new UnsafeByteString("set"), null);

        CompletableFuture<byte[]> future = new CompletableFuture<>();
        handler.handle(request, null, future);
        verify(nextHandler, times(1)).handle(eq(request), any(), eq(future));
    }

}
