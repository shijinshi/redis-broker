package cn.shijinshi.redis.forward.client;

import java.util.concurrent.CompletableFuture;

/**
 * @author Gui Jiahai
 */
public class RequestAndFuture {

    private final Object request;
    private final CompletableFuture<byte[]> future;

    public RequestAndFuture(Object request, CompletableFuture<byte[]> future) {
        this.request = request;
        this.future = future;
    }

    public Object getRequest() {
        return request;
    }

    public CompletableFuture<byte[]> getFuture() {
        return future;
    }

}
