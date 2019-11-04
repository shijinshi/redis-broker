package cn.shijinshi.redis.common.log;

/**
 * @author Gui Jiahai
 */
public class LogProperties {

    private String dir = System.getProperty("java.io.tmpdir");
    private long cleanSec = 60;
    private long segmentBytes = 128 * 1024 * 1024;
    private long persistIndexMs = 1000;

    public String getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public long getCleanSec() {
        return cleanSec;
    }

    public void setCleanSec(long cleanSec) {
        this.cleanSec = cleanSec;
    }

    public long getSegmentBytes() {
        return segmentBytes;
    }

    public void setSegmentBytes(long segmentBytes) {
        this.segmentBytes = segmentBytes;
    }

    public long getPersistIndexMs() {
        return persistIndexMs;
    }

    public void setPersistIndexMs(long persistIndexMs) {
        this.persistIndexMs = persistIndexMs;
    }
}
