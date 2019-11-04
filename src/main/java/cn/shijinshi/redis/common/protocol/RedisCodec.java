package cn.shijinshi.redis.common.protocol;

import cn.shijinshi.redis.common.util.UnsafeByteString;
import io.netty.util.concurrent.FastThreadLocal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * 用于解析Redis的请求和响应报文。
 *
 * @see <a href="http://doc.redisfans.com/topic/protocol.html">Redis通信协议</a>
 * @author Gui Jiahai
 */
public class RedisCodec {

    private static final FastThreadLocal<Temp> tempMap = new FastThreadLocal<Temp>() {
        @Override
        protected Temp initialValue() {
            return new Temp();
        }
    };

    /**
     * 解析Redis请求报文。
     *
     * @return 若返回不为null，则表示成功地读取到一个Redis请求报文
     *          若返回null，则表示未能读取到完整的报文，需要等待更多的数据
     * @throws IOException 当报文的格式错误或者input异常时，会抛出IOException
     */
    /*
    请求报文格式为

    *<参数数量> CR LF
    $<参数 1 的字节数量> CR LF
    <参数 1 的数据> CR LF
    ...
    $<参数 N 的字节数量> CR LF
    <参数 N 的数据> CR LF
     */
    public static RedisRequest decodeRequest(InputStream input) throws IOException {
        int s1 = 0, s2 = 0, t1 = 0, t2 = 0, t = 0;
        int b;

        Temp temp = tempMap.get().reset();
        if ((b = check(input.read(), '*')) == -1) {
            return null;
        }
        temp.byteArray.write(b);

        if ((t = readInteger(input, temp)) == -1) {
            return null;
        }
        int rows = temp.integer;
        if (rows <= 0) {
            throw new IOException("empty request");
        }

        s1 = t + 3;
        for (int i = 0; i < rows; i ++) {
            if ((b = check(input.read(), '$')) == -1) {
                return null;
            }
            temp.byteArray.write(b);

            if ((t = readBulk(input, temp)) == -1) {
                return null;
            }
            if (i == 0) {
                s1 += (t + 1 - temp.len);
                t1 = temp.len;
            } else if (i == 1) {
                s2 = s1 + t1 + 2 + (t + 1 - temp.len);
                t2 = temp.len;
            }
        }

        byte[] content = temp.byteArray.toByteArray();
        UnsafeByteString command, subCommand = null;
        if (t1 <= 0) {
            throw new IOException("wrong format redis request");
        }
        command = new UnsafeByteString(content, s1, t1);
        if (t2 > 0) {
            subCommand = new UnsafeByteString(content, s2, t2);
        }

        return new RedisRequest(content, command, subCommand);
    }

    /**
     * 解析Redis回复报文
     *
     * @return 如果返回不为null，则表示成功地读取到一个回复报文
     *          若返回null，则表示未能读取到完整的报文，需要等待更多的数据
     * @throws IOException 当报文的格式错误或者input异常时，会抛出IOException
     */
    public static byte[] decodeReply(InputStream input) throws IOException {
        Temp temp = tempMap.get().reset();
        if (read0(input, temp) == -1) {
            return null;
        }
        return temp.byteArray.toByteArray();
    }

    private static int read0(InputStream input, Temp temp) throws IOException {
        int b = input.read();
        if (b == -1) {
            return -1;
        }
        temp.byteArray.write(b);

        switch (b) {
            case '+':
            case ':':
            case '-':
                return readLine(input, temp);
            case '$':
                return readBulk(input, temp);
            case '*':
                return readMultiBulk(input, temp);
            default:
                throw new IOException("Unknown reply with (byte)" + b);
        }
    }

    /**
     * 多条批量回复（multi bulk reply）
     *
     * @return 返回批量的数量
     */
    private static int readMultiBulk(InputStream input, Temp temp) throws IOException {
        if (readInteger(input, temp) == -1) {
            return -1;
        }
        /*
            在多条批量回复(multi bulk reply)中，
            *0\r\n 代表空白的，
            *-1\r\n 代表空的，
            这两者类似于字符串中 "" 和 null 的区别
        */
        int rows = temp.integer;
        if (rows <= 0) {
            return 0;
        }
        for (int i = 0; i < rows; i ++) {
            if (read0(input, temp) == -1) {
                return -1;
            }
        }
        return rows;
    }

    /**
     * 批量回复（bulk reply）
     * @return 返回整条回复的字节数(不包括第一个标识字节$和最后的\r\n)
     */
    private static int readBulk(InputStream input, Temp temp) throws IOException {
        int len;
        if ((len = readInteger(input, temp)) == -1) {
            return -1;
        }
        /*
            如果被请求的值不存在， 那么批量回复会将特殊值 -1 用作回复的长度值
            当请求对象不存在时，客户端应该返回空对象
         */
        if (temp.integer == -1) {
            return len;
        }
        len += (temp.integer + 2);
        temp.len = temp.integer;

        int b;
        for (int i = 0; i < temp.integer; i ++) {
            if ((b = input.read()) != -1) {
                temp.byteArray.write(b);
            } else {
                return -1;
            }
        }

        if (check(input.read(), '\r') == -1 || check(input.read(), '\n') == -1) {
            return -1;
        }
        temp.byteArray.write('\r');
        temp.byteArray.write('\n');
        return len;
    }

    /**
     * 读取一行，直到遇到 \r\n
     * 可用于 状态回复（status reply）, 错误回复（error reply）, 整数回复（integer reply）
     *
     * @return 返回整条回复的字节数(不包括第一个标识字节+-:，包括\r\n)
     */
    private static int readLine(InputStream input, Temp temp) throws IOException {
        int b, len = 0;
        while ((b = input.read()) != -1) {
            len ++;
            temp.byteArray.write(b);
            if (b == '\r') {
                if ((b = input.read()) == -1) {
                    break;
                }
                len ++;
                temp.byteArray.write(b);
                if (b == '\n') {
                    return len;
                }
            }
        }
        return -1;
    }

    /**
     * 读取一个整数，直到遇到 \r\n
     * @return 返回整条回复的字节数(不包括第一个标识字节和\r\n)
     */
    private static int readInteger(InputStream input, Temp temp) throws IOException {
        temp.integer = 0;
        int b, len = 0;
        if ((b = input.read()) == -1) {
            return -1;
        } else if (b == '-') {
            len ++;
            temp.byteArray.write(b);
            while ((b = input.read()) != -1) {
                len ++;
                temp.byteArray.write(b);
                if (b == '\r') {
                    if (check(input.read(), '\n') == -1) {
                        return -1;
                    }
                    len --;
                    temp.byteArray.write('\n');
                    temp.integer *= -1;
                    return len;
                } else {
                    temp.integer = temp.integer * 10 + (b - '0');
                }
            }
        } else {
            do {
                len ++;
                temp.byteArray.write(b);
                if (b == '\r') {
                    if (check(input.read(), '\n') == -1) {
                        return -1;
                    }
                    len --;
                    temp.byteArray.write('\n');
                    return len;
                } else {
                    temp.integer = temp.integer * 10 + (b - '0');
                }
            } while ((b = input.read()) != -1);
        }
        return -1;
    }


    private static int check(int actual, int expect) throws IOException {
        if (actual == -1) {
            return -1;
        }
        if (actual != expect) {
            String msg = String.format("expect [%s] but [%s]", charReadable((char) expect), charReadable((char) actual));
            throw new IOException(msg);
        }
        return actual;
    }

    private static String charReadable(char ch) {
        if (ch == '\n') {
            return "\\n";
        } else if (ch == '\r') {
            return "\\r";
        } else if (ch == '\0') {
            return "NULL";
        } else if (ch == '\t') {
            return "\\t";
        }
        return String.valueOf(ch);
    }

    static class Temp {
        private final ByteArrayOutputStream byteArray = new ByteArrayOutputStream(128);
        private int integer = -1, len = -1;
        Temp reset() {
            byteArray.reset();
            integer = len = -1;
            return this;
        }
    }


}
