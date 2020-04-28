package net.noboard.saturn;

import java.util.Collection;

public interface DataPool<T> {
    Collection<T> read(int pageNum, int pageSize);

    void afterLastReadElement(int pageNum, int pageSize, int count);

    void beforeFirstReadElement();
}
