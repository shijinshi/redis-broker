package cn.shijinshi.redis.forward.handler;

import cn.shijinshi.redis.common.protocol.RedisRequest;
import cn.shijinshi.redis.control.broker.Broker;
import cn.shijinshi.redis.forward.Handler;
import cn.shijinshi.redis.forward.support.CommandSupports;
import cn.shijinshi.redis.forward.support.Support;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * 对请求报文进行检查
 * 部分命令不需要交给Redis处理，比如：
 * CLUSTER SLOTS
 *
 * @author Gui Jiahai
 */
public class SupportHandler implements Handler {

    private static final String template = "-ERR Unsupported command '%s'\r\n";

    private final Handler next;
    private final CommandSupports commandSupports;
    private final Broker broker;

    public SupportHandler(Handler next, Broker broker, CommandSupports commandSupports) {
        this.next = next;
        this.broker = broker;
        this.commandSupports = commandSupports;
    }

    @Override
    public void handle(RedisRequest request, Support support, CompletableFuture<byte[]> future) {
        if (support == null) {
            support = commandSupports.get(request.getCommand());
        }
        if (support == null) {
            String reply = String.format(template, request.getCommand().toString());
            future.complete(reply.getBytes());
        } else if (support.isBackup() && !broker.isMaster()) {
            String reply = "-ERR READONLY You can't write against a read only slave.\r\n";
            future.complete(reply.getBytes());
        } else if (support.getPreparedAction() == null || !support.getPreparedAction().apply(request, future)) {
            this.next.handle(request, support, future);
        }
    }

    @Override
    public void close() {
        try {
            this.next.close();
        } catch (IOException ignored) {
        }
    }
}
