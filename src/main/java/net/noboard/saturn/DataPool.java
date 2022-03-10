package net.noboard.saturn;

import java.util.Collection;

@FunctionalInterface
public interface DataPool<T> {
    Collection<T> read(int pageNum, int pageSize);

    /**
     * 该方法会在最后一个元素被读出时触发
     *
     * @param pageNum
     * @param pageSize
     * @param count
     */
    default void afterLastElementRead(int pageNum, int pageSize, int count) {

    }

    /**
     * 该方法会在第一次read方法执行前触发
     */
    default void beforeFirstElementRead() {

    }
}
