package cn.shijinshi.redis.common.log;

/**
 * @author Gui Jiahai
 */
public class IndexEntry {

    private final long sequence;
    private final long offset;

    public IndexEntry(long sequence, long offset) {
        this.sequence = sequence;
        this.offset = offset;
    }

    public long getSequence() {
        return sequence;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof IndexEntry) {
            IndexEntry another = (IndexEntry) obj;
            return this.sequence == another.sequence && this.offset == another.offset;
        }
        return false;
    }

    @Override
    public String toString() {
        return "IndexEntry{" +
                "sequence=" + sequence +
                ", offset=" + offset +
                '}';
    }
}
