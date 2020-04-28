package net.noboard.saturn;

import java.util.Iterator;

/**
 * 订阅相关数据读取监听器
 */
public class DataReader<T> implements Iterator<T> {

    private int index = 0;

    private boolean hasNext;

    private DataInfo<T> current;

    private DataChannel<T> dataChannel;

    DataReader(DataChannel<T> dataChannel) {
        this.hasNext = true;
        this.dataChannel = dataChannel;
    }

    @Override
    public boolean hasNext() {
        if (index == 0) {
            hasNext = dataChannel.get(index) != null;
        }

        current = dataChannel.get(index);

        return hasNext;
    }

    @Override
    public T next() {
        DataInfo<T> dataInfo = dataChannel.get(this.index++);
        hasNext = dataInfo.isHasNext();
        return dataInfo.getData();
    }
}
