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

    private static final int DEFAULT_PAGE_SIZE = 100;

    private static final int DEFAULT_CONCURRENT_READ_NUM = 1;

    private Integer pageSize;

    @SafeVarargs
    private Saturn(int pageSize, int concurrentReadNum, DataPool<T>... dataPools) {
        if (dataPools == null || dataPools.length == 0) {
            return;
        }

        this.pageSize = pageSize;

        for (DataPool<T> dataPool : dataPools) {
            if (dataPool == null) {
                continue;
            }
            dataReaders.addLast(DataChannel.connect(dataPool, pageSize, concurrentReadNum));
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

        return new Saturn<T>(pageSize, DEFAULT_CONCURRENT_READ_NUM ,dataPools);
    }

    @SafeVarargs
    public static <T> Saturn<T> connect(int pageSize, int concurrentReadNum, DataPool<T>... dataPools) {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("page size must be a positive integer");
        }
        if (concurrentReadNum <= 0) {
            throw new IllegalArgumentException("concurrentReadNum size must be a positive integer");
        }

        return new Saturn<T>(pageSize, concurrentReadNum ,dataPools);
    }


    @SafeVarargs
    public static <T> Saturn<T> connect(DataPool<T>... dataPools) {
        return new Saturn<T>(DEFAULT_PAGE_SIZE, DEFAULT_CONCURRENT_READ_NUM, dataPools);
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
        return () -> new Itr(dataReaders);
    }

    public <R, A> Iterable<R> forEachBatch(int size, Collector<? super T, A, R> collector) {
        if (size <= 0) {
            throw new IllegalArgumentException("batch size must be a positive integer");
        }

        return () -> new Itr4Collection<>(size, collector, dataReaders);
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

    /**
     * 元素遍历器
     */
    private class Itr implements Iterator<T> {

        private final LinkedList<DataChannel<T>> dataReaders;

        private int index = -1;

        private Iterator<T> current;

        public Itr(LinkedList<DataChannel<T>> dataReaders) {
            this.dataReaders = dataReaders;
        }

        @Override
        public boolean hasNext() {
            return checkNext();
        }

        @Override
        public T next() {
            return current.next();
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
            if (this.current == null) {
                return false;
            }

            return this.current.hasNext();
        }

        private boolean toNextReader() {
            if ((this.index + 1) < this.dataReaders.size()) {
                this.current = this.dataReaders.get(++this.index).iterator();
                return true;
            } else {
                return false;
            }
        }

    }

    /**
     * 元素
     * @param <R>
     * @param <A>
     */
    private class Itr4Collection<R,A> implements Iterator<R> {

        private final Collector<? super T, A, R> collector;

        private final int size;

        private final Iterator<T> iterator;

        public Itr4Collection(int size, Collector<? super T, A, R> collector, LinkedList<DataChannel<T>> dataReaders) {
            this.collector = collector;
            this.size = size;
            iterator = new Itr(dataReaders);
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public R next() {
            int i = 0;
            A container = this.collector.supplier().get();
            while (this.iterator.hasNext() && i++ < this.size) {
                this.collector.accumulator().accept(container, this.iterator.next());
            }
            if (this.collector.characteristics().contains(Collector.Characteristics.IDENTITY_FINISH)) {
                return (R) container;
            } else {
                return collector.finisher().apply(container);
            }
        }
    }
}
