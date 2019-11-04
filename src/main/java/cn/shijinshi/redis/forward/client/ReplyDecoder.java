package cn.shijinshi.redis.forward.client;

import cn.shijinshi.redis.common.protocol.RedisCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.InputStream;
import java.util.List;

/**
 * 解析来自Redis的响应报文。
 *
 * @author Gui Jiahai
 */
public class ReplyDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        InputStream input = new ByteBufInputStream(in);
        int saveReaderIndex;
        byte[] reply;
        do {
            saveReaderIndex = in.readerIndex();
            reply = RedisCodec.decodeReply(input);
            if (reply == null) {
                in.readerIndex(saveReaderIndex);
                break;
            } else {
                out.add(reply);
            }
        } while (in.isReadable());
    }
}
