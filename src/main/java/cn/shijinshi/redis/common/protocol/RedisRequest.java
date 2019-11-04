package cn.shijinshi.redis.common.protocol;

import cn.shijinshi.redis.common.util.UnsafeByteString;

/**
 * Redis的请求报文格式。
 *
 * {@link RedisCodec}会将请求报文解析为RedisRequest格式。
 *
 * @author Gui Jiahai
 */
public class RedisRequest {

    /**
     * 请求报文所包含的完整字节数组
     */
    private final byte[] content;

    /**
     * 请求对应的命令。
     * 例如: {@code SET KEY VALUE}，那么{@code command }对应着 {@code SET}
     */
    private final UnsafeByteString command;

    /**
     * 请求对应的子命令。
     * 当请求命令为多条批量时，如 {@code LRANGE mylist 0 3}
     * 那么子命令对应着第二个字符串，即 {@code mylist}
     */
    private final UnsafeByteString subCommand;

    public RedisRequest(byte[] content, UnsafeByteString command, UnsafeByteString subCommand) {
        this.content = content;
        this.command = command;
        this.subCommand = subCommand;
    }

    public byte[] getContent() {
        return content;
    }

    public UnsafeByteString getCommand() {
        return command;
    }

    public UnsafeByteString getSubCommand() {
        return subCommand;
    }
}
