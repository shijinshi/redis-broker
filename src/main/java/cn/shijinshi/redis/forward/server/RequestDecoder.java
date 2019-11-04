package cn.shijinshi.redis.forward.server;

import cn.shijinshi.redis.common.protocol.RedisCodec;
import cn.shijinshi.redis.common.protocol.RedisRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 解析来自客户端的请求报文
 *
 * @author Gui Jiahai
 */
public class RequestDecoder extends ByteToMessageDecoder {

    private static final Logger logger = LoggerFactory.getLogger(RequestDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (!ctx.channel().isActive()) {
            return;
        }

        InputStream input = new ByteBufInputStream(in);
        int saveReaderIndex;
        RedisRequest request;
        do {
            saveReaderIndex = in.readerIndex();
            try {
                request = RedisCodec.decodeRequest(input);
            } catch (IOException e) {
                in.readerIndex(saveReaderIndex);
                logger.error("Bad redis bytes stream:\n{}", in.toString(StandardCharsets.UTF_8));
                throw e;
            }

            if (request == null) {
                in.readerIndex(saveReaderIndex);
                break;
            } else {
                out.add(request);
            }
        } while (in.isReadable());
    }
}
