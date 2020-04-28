package net.noboard.saturn;

public class DataInfo<T> {
    private T data;

    private boolean hasNext;

    public DataInfo(T data, boolean hasNext) {
        this.data = data;
        this.hasNext = hasNext;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public boolean isHasNext() {
        return hasNext;
    }

    public void setHasNext(boolean hasNext) {
        this.hasNext = hasNext;
    }
}