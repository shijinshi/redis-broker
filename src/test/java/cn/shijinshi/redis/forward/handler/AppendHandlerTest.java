package cn.shijinshi.redis.forward.handler;

import cn.shijinshi.redis.common.protocol.RedisRequest;
import cn.shijinshi.redis.forward.Handler;
import cn.shijinshi.redis.forward.support.Support;
import cn.shijinshi.redis.sync.Appender;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.*;

/**
 * @author Gui Jiahai
 */
@RunWith(MockitoJUnitRunner.class)
public class AppendHandlerTest {

    @InjectMocks
    private AppendHandler handler;

    @Mock
    private Appender appender;
    @Mock
    private Handler nextHandler;

    private RedisRequest request = new RedisRequest(null, null, null);

    @Test
    public void test_success() throws IOException {
        Support support = new Support(true, null);

        CompletableFuture<byte[]> future = new CompletableFuture<>();
        handler.handle(request, support, future);
        future.complete("+PONG\r\n".getBytes());

        verify(appender).append(request);
        verify(nextHandler).handle(request, support, future);
    }

    @Test
    public void test_fail() {
        Support support = new Support(true, null);

        CompletableFuture<byte[]> future = new CompletableFuture<>();
        handler.handle(request, support, future);
        future.complete("-PONG\r\n".getBytes());

        verifyZeroInteractions(appender);
        verify(nextHandler).handle(request, support, future);
    }

    @Test
    public void test_unsupported() {
        Support support = new Support(false, null);

        CompletableFuture<byte[]> future = new CompletableFuture<>();
        handler.handle(request, support, future);
        future.complete("+PONG\r\n".getBytes());

        verifyZeroInteractions(appender);
        verify(nextHandler).handle(request, support, future);
    }

}
