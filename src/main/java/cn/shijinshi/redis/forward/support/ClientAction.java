package cn.shijinshi.redis.forward.support;

import cn.shijinshi.redis.common.Constants;
import cn.shijinshi.redis.common.protocol.RedisRequest;
import cn.shijinshi.redis.common.util.UnsafeByteString;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

/**
 * 屏蔽CLIENT LIST 和 CLIENT SETNAME 两个命令
 *
 * @author Gui Jiahai
 */
@Component("client")
@Lazy
public class ClientAction implements Action {

    private static final UnsafeByteString LIST_SUB_COMMAND = new UnsafeByteString("list".toLowerCase());
    private static final UnsafeByteString SETNAME_SUB_COMMAND = new UnsafeByteString("setname".toLowerCase());

    @Override
    public Boolean apply(RedisRequest redisRequest, CompletableFuture<byte[]> future) {
        if (LIST_SUB_COMMAND.equals(redisRequest.getSubCommand())) {
            future.complete(clientListBytes());
        } else if (SETNAME_SUB_COMMAND.equals(redisRequest.getSubCommand())) {
            future.complete(Constants.OK_REPLY);
        } else {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    private byte[] clientListBytes() {
        String list = "id=864382 addr=127.0.0.1:0 fd=41 name= age=8856 idle=8856 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 " +
                "qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=NULL\n";
        String sb = "$" + list.length() + String.valueOf(Constants.CRLF_CHAR) +
                list + String.valueOf(Constants.CRLF_CHAR);
        return sb.getBytes(StandardCharsets.UTF_8);
    }
}
