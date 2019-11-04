package cn.shijinshi.redis.common.protocol;

import cn.shijinshi.redis.common.util.UnsafeByteString;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * @author Gui Jiahai
 */
public class RedisCodecTest {

    private final String requestStr0 = "*3\r\n";

    private final String requestStr1 = "*3\r\n$3\r\nSET\r\n$4\r\nNAME\r\n$4\r\nJACK\r\n";

    @Test
    public void test_request() throws IOException {
        String requestString = requestStr1 + requestStr0;
        ByteArrayInputStream input = new ByteArrayInputStream(requestString.getBytes());

        RedisRequest request;

        request = RedisCodec.decodeRequest(input);
        Assert.assertNotNull(request);
        Assert.assertEquals(request.getCommand(), new UnsafeByteString("SET"));
        Assert.assertEquals(request.getSubCommand(), new UnsafeByteString("NAME"));
        Assert.assertArrayEquals(request.getContent(), requestStr1.getBytes());

        request = RedisCodec.decodeRequest(input);
        Assert.assertNull(request);
    }

    @Test(expected = IOException.class)
    public void test_request_illegal() throws IOException {
        String requestStr = "*3\r\n$3\r\nSET\r\n$4\r\nNAME\r\n$4\r\nJACK\n\r";
        ByteArrayInputStream input = new ByteArrayInputStream(requestStr.getBytes());

        RedisCodec.decodeRequest(input);
    }

    @Test
    public void test_reply_multiBulk() throws IOException {
        test_reply(requestStr1);
    }

    @Test
    public void test_reply_status() throws IOException {
        String requestStr = "+PING\r\n";
        test_reply(requestStr);
    }

    @Test
    public void test_reply_error() throws IOException {
        String requestStr = "-ERR error\r\n";
        test_reply(requestStr);
    }

    @Test
    public void test_reply_integer() throws IOException {
        String requestStr = ":404\r\n";
        test_reply(requestStr);
    }

    @Test
    public void test_reply_bulk() throws IOException {
        String requestStr = "$10\r\nhelloWorld\r\n";
        test_reply(requestStr);
    }

    @Test(expected = IOException.class)
    public void test_reply_illegal() throws IOException {
        String requestStr = "$11\r\nhelloWorld\r\n";
        test_reply(requestStr);
    }


    private void test_reply(String requestStr) throws IOException {
        String requestString = requestStr + requestStr0;
        ByteArrayInputStream input = new ByteArrayInputStream(requestString.getBytes());

        byte[] reply;

        reply = RedisCodec.decodeReply(input);
        Assert.assertNotNull(reply);
        Assert.assertArrayEquals(reply, requestStr.getBytes());

        reply = RedisCodec.decodeReply(input);
        Assert.assertNull(reply);
    }

}
