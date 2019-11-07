package de.hhu.bsinfo.dxram.loading;

public class Pair<T> {
    private T from;
    private T to;

    public Pair(T from, T to) {
        this.from = from;
        this.to = to;
    }

    public T getFrom() {
        return from;
    }

    public T getTo() {
        return to;
    }
}
