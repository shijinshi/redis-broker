package cn.shijinshi.redis.common.util;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * 为了避免频繁使用字节数组创建String，特引入此类。
 * 使用者需要注意，UnsafeByteString会直接持有原字节数组，
 * 所以，如果直接修改原数组，会对UnsafeByteString有影响的。
 * 所以，UnsafeByteString是不安全的。
 *
 * 另外，从实现中可以看到，在计算hash或者equals时，不区分大小写
 *
 * @author Gui Jiahai
 */
public final class UnsafeByteString implements Serializable {

    private final byte[] value;
    private final int offset;
    private final int len;

    private transient int hash = 0;
    private transient String string;

    public UnsafeByteString(String string) {
        this(string.getBytes(StandardCharsets.UTF_8));
    }

    public UnsafeByteString(byte[] value) {
        this(value, 0, value.length);
    }

    public UnsafeByteString(byte[] value, int offset, int len) {
        this.value = value;
        this.offset = offset;
        this.len = len;
    }

    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0 && len > 0) {
            int end = len + offset;
            for (int i = offset; i < end; i++) {
                h = 31 * h + Character.toLowerCase(value[i]);
            }
            hash = h;
        }
        return h;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof UnsafeByteString) {
            UnsafeByteString another = (UnsafeByteString) obj;
            int n = len;
            if (n == another.len) {
                int i = 0;
                while (n-- != 0) {
                    byte u1 = value[offset + i];
                    byte u2 = another.value[another.offset + i];
                    if (u1 != u2 && Character.toLowerCase(u1) != Character.toLowerCase(u2)) {
                        return false;
                    }
                    i++;
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        if (string == null) {
            string = new String(value, offset, len);
        }
        return string;
    }

    public byte[] getBytes() {
        return Arrays.copyOfRange(value, offset, offset + len);
    }

}
