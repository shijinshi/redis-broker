package cn.shijinshi.redis.common.param;

import java.io.Serializable;

/**
 * @author Gui Jiahai
 */
public class Range implements Serializable, Comparable<Range> {

    private int lowerBound;
    private int upperBound;

    public Range() {
    }

    public Range(int lowerBound, int upperBound) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public void setLowerBound(int lowerBound) {
        this.lowerBound = lowerBound;
    }

    public int getLowerBound() {
        return lowerBound;
    }

    public void setUpperBound(int upperBound) {
        this.upperBound = upperBound;
    }

    public int getUpperBound() {
        return upperBound;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Range)) {
            return false;
        }

        Range that = (Range) obj;
        return this.upperBound == that.upperBound && this.lowerBound == that.lowerBound;
    }

    @Override
    public String toString() {
        return "Range{" +
                "lowerBound=" + lowerBound +
                ", upperBound=" + upperBound +
                '}';
    }

    @Override
    public int compareTo(Range o) {
        return this.lowerBound - o.lowerBound;
    }
}
