package de.hhu.bsinfo.dxram.loading;

public class LongLongPair {
    private long a;
    private long b;

    public LongLongPair(long a, long b) {
        this.a = a;
        this.b = b;
    }

    public long getOne() {
        return a;
    }

    public void setOne(long a) {
        this.a = a;
    }

    public long getTwo() {
        return b;
    }

    public void setTwo(long b) {
        this.b = b;
    }
}
