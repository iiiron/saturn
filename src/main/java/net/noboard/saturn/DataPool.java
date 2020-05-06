package net.noboard.saturn;

import java.util.Collection;

@FunctionalInterface
public interface DataPool<T> {
    Collection<T> read(int pageNum, int pageSize);

    default void afterLastElementRead(int pageNum, int pageSize, int count) {

    }

    default void beforeFirstElementRead() {

    }
}
