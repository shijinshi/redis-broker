package cn.shijinshi.redis.control.rpc;

import cn.shijinshi.redis.common.protocol.RedisCodec;
import cn.shijinshi.redis.common.protocol.RedisRequest;
import cn.shijinshi.redis.common.serialize.Serializer;
import cn.shijinshi.redis.forward.RedisConnector;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static cn.shijinshi.redis.common.Constants.CRLF_BYTE;

/**
 * @author Gui Jiahai
 */
@Component
@Lazy
public class RpcHelper {

    private static final byte[] RPC_COMMAND = "rpc".getBytes();

    private final Serializer serializer;

    public RpcHelper(Serializer serializer) {
        this.serializer = serializer;
    }

    public CompletableFuture<Answer> sendIndication(Indication indication, RedisConnector connector) {
        byte[] request;
        try {
            request = toRequest(indication);
        } catch (IOException e) {
            CompletableFuture<Answer> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        connector.send(request, future);
        return future.thenApply(bytes -> {
            try {
                return fromResponse(bytes);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    public byte[] toRequest(Indication indication) throws IOException {
        byte[] result = serializer.serialize(indication);
        try {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            stream.write('*');
            stream.write('2');
            stream.write(CRLF_BYTE);
            stream.write('$');
            stream.write('3');
            stream.write(CRLF_BYTE);
            stream.write(RPC_COMMAND);
            stream.write(CRLF_BYTE);
            stream.write('$');
            stream.write(String.valueOf(result.length).getBytes());
            stream.write(CRLF_BYTE);
            stream.write(result);
            stream.write(CRLF_BYTE);
            return stream.toByteArray();
        } catch (IOException ignored) {
        }
        return null;
    }

    public byte[] toResponse(Answer answer) throws IOException {
        byte[] result = serializer.serialize(answer);
        byte[] content = null;
        try {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            stream.write('$');
            stream.write(String.valueOf(result.length).getBytes());
            stream.write(CRLF_BYTE);
            stream.write(result);
            stream.write(CRLF_BYTE);
            content = stream.toByteArray();
        } catch (IOException ignored) {
        }
        return content;
    }

    public Indication fromRequest(RedisRequest request) throws IOException {
        if (request.getSubCommand() == null) {
            ByteArrayInputStream stream = new ByteArrayInputStream(request.getContent());
            RedisRequest r = RedisCodec.decodeRequest(stream);
            if (r == null) {
                throw new IOException("Incomplete redis request message");
            }
            request = r;
        }
        if (request.getSubCommand() == null) {
            throw new IOException("Cannot get subCommand from request bytes");
        }
        return serializer.deserialize(request.getSubCommand().getBytes(), Indication.class);
    }

    public Answer fromResponse(byte[] data) throws IOException {
        if (data[0] == '-') {
            throw new IOException(new String(data, 1, data.length - 3));
        }

        if (data[0] != '$') {
            throw new IOException("Wrong format data, the first byte must be $");
        }
        int index = 2;
        byte[] lenBytes = null;
        for (; index < data.length; index ++) {
            if (data[index] == '\n' && data[index - 1] == '\r') {
                lenBytes = Arrays.copyOfRange(data, 1, index - 1);
                break;
            }
        }
        if (lenBytes == null) {
            throw new IOException("Wrong format data, cannot find the bytes of length");
        }
        String s = new String(lenBytes);
        int len = Integer.parseInt(s);
        if (len <= 0) {
            throw new IOException("Wrong format data, the length of answer bytes is " + len);
        }

        byte[] content = Arrays.copyOfRange(data, index + 1, index + 1 + len);
        return serializer.deserialize(content, Answer.class);
    }

}
