package cn.shijinshi.redis.common.log;

/**
 *
 *
 * @author Gui Jiahai
 */
public enum Access {

    R,

    W,

    RW;

    public boolean isReadable() {
        return this == R || this == RW;
    }

    public boolean isWritable() {
        return this == W || this == RW;
    }

}
