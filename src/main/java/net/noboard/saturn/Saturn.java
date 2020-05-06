package net.noboard.saturn;


import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * 暂时只能顺序的读取一遍
 *
 * @param <T>
 */
public class Saturn<T> {

    private final LinkedList<DataChannel<T>> dataReaders = new LinkedList<>();

    private int index = -1;

    private DataChannel<T> current;

    private final Integer pageSize = 100;

    @SafeVarargs
    private Saturn(Integer pageSize, DataPool<T>... dataPools) {
        if (dataPools == null || dataPools.length == 0) {
            return;
        }

        for (DataPool<T> dataPool : dataPools) {
            if (dataPool == null) {
                continue;
            }
            if (pageSize == null || pageSize <= 0) {
                dataReaders.addLast(DataChannel.connect(dataPool, this.pageSize));
            } else {
                dataReaders.addLast(DataChannel.connect(dataPool, pageSize));
            }
        }
    }

    /**
     * 链接到数据源
     *
     * @param pageSize  分页时每页长度
     * @param dataPools 数据源
     * @return
     */
    @SafeVarargs
    public static <T> Saturn<T> connect(int pageSize, DataPool<T>... dataPools) {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("page size must be a positive integer");
        }

        return new Saturn<T>(pageSize, dataPools);
    }


    @SafeVarargs
    public static <T> Saturn<T> connect(DataPool<T>... dataPools) {
        return new Saturn<T>(null, dataPools);
    }

    public <R, A> R getAll(Collector<? super T, A, R> collector) {
        A container = collector.supplier().get();
        for (T t : forEach()) {
            collector.accumulator().accept(container, t);
        }
        if (collector.characteristics().contains(Collector.Characteristics.IDENTITY_FINISH)) {
            return (R) container;
        } else {
            return collector.finisher().apply(container);
        }
    }

    public List<T> getAllToList() {
        return getAll(Collectors.toList());
    }

    public Set<T> getAllToSet() {
        return getAll(Collectors.toSet());
    }

    public Iterable<T> forEach() {
        return () -> new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return checkNext();
            }

            @Override
            public T next() {
                return current.next();
            }
        };
    }

    public <R, A> Iterable<R> forEachBatch(int size, Collector<? super T, A, R> collector) {
        if (size <= 0) {
            throw new IllegalArgumentException("batch size must be a positive integer");
        }

        return () -> new Iterator<R>() {
            @Override
            public boolean hasNext() {
                return checkNext();
            }

            @Override
            public R next() {
                int i = 0;
                A container = collector.supplier().get();
                while (checkNext() && i++ < size) {
                    collector.accumulator().accept(container, current.next());
                }
                if (collector.characteristics().contains(Collector.Characteristics.IDENTITY_FINISH)) {
                    return (R) container;
                } else {
                    return collector.finisher().apply(container);
                }
            }
        };
    }

    public <R, A> Iterable<R> forEachBatch(Collector<? super T, A, R> collector) {
        return forEachBatch(this.pageSize, collector);
    }

    public <R, A> Iterable<List<T>> forEachBatchToList() {
        return forEachBatch(this.pageSize, Collectors.toList());
    }

    public <R, A> Iterable<List<T>> forEachBatchToList(int size) {
        return forEachBatch(size, Collectors.toList());
    }

    public <R, A> Iterable<Set<T>> forEachBatchToSet() {
        return forEachBatch(this.pageSize, Collectors.toSet());
    }

    public <R, A> Iterable<Set<T>> forEachBatchToSet(int size) {
        return forEachBatch(size, Collectors.toSet());
    }

    private boolean checkNext() {
        do {
            if (isCurrentHasNext()) {
                return true;
            }
        } while (toNextReader());

        return false;
    }

    private boolean isCurrentHasNext() {
        if (current == null) {
            return false;
        }

        return current.hasNext();
    }

    private boolean toNextReader() {
        if ((index + 1) < dataReaders.size()) {
            current = dataReaders.get(++index);
            return true;
        } else {
            return false;
        }
    }
}
