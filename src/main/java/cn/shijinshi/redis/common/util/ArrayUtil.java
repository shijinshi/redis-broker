package cn.shijinshi.redis.common.util;

/**
 * @author Gui Jiahai
 */
public final class ArrayUtil {

    private ArrayUtil() {}

    /**
     * 检查字节数组是否以特定的字节数组开始
     */
    public static boolean startsWith(byte[] source, byte[] prefix) {
        if (source == prefix) {
            return true;
        }

        if (source == null || prefix == null) {
            return false;
        }

        int len = prefix.length;
        if (source.length < len) {
            return false;
        }

        for (int i = 0; i < len; i ++) {
            if (source[i] != prefix[i]) {
                return false;
            }
        }

        return true;
    }

}
