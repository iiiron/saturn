package net.noboard.saturn;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class DataChannel<T> implements Iterable<T> {

    private final DataPool<T> dataPool;

    private final int concurrentReadNum;

    /**
     * 分页长度
     */
    private final int holdPageSize;

    private final Map<Integer, Map<Integer, T>> cacheData;

    private final ArrayDeque<Integer> cachePageDeque;

    private DataChannel(DataPool<T> dataPool, int pageSize, int concurrentReadNum) {
        this.dataPool = dataPool;
        this.cacheData = new HashMap<>();
        this.cachePageDeque = new ArrayDeque<>();
        this.holdPageSize = pageSize;
        this.concurrentReadNum = concurrentReadNum;
    }

    public static <T> DataChannel<T> connect(DataPool<T> dataPool, int pageSize, int concurrentReadNum) {
        return new DataChannel<>(dataPool, pageSize, concurrentReadNum);
    }

    private synchronized DataInfo<T> get(int index) {
        int pageNum = calcPage(index, holdPageSize);

        Map<Integer, T> pageData = loadPageData(pageNum);
        if (pageData == null) {
            return null;
        }

        boolean hasNext = false;
        int nextPageNum = calcPage(index + 1, holdPageSize);
        if (nextPageNum != pageNum) {
            Map<Integer, T> nextPageData = loadPageData(nextPageNum);
            if (nextPageData != null && nextPageData.size() > 0) {
                hasNext = true;
            }
        } else if ((pageData.size() - 1) > calcRelativeIndex(index, holdPageSize)) {
            hasNext = true;
        }

        return new DataInfo<>(pageData.get(index), hasNext);
    }

    private Map<Integer, T> loadPageData(int pageNum) {
        if (cacheData.get(pageNum) == null) {
            List<CompletableFuture<Map<Integer, T>>> completableFutures = new ArrayList<>();
            for (int i = 0; i < concurrentReadNum; i++) {
                int currentPageNum = pageNum + i;
                completableFutures.add(CompletableFuture.supplyAsync(() -> {
                    Collection<T> data = dataPool.read(currentPageNum, holdPageSize);
                    if (data == null || data.size() == 0) {
                        return null;
                    }

                    Map<Integer, T> pageDataTamp = new HashMap<>();
                    int index = (currentPageNum - 1) * holdPageSize;
                    for (T t : data) {
                        pageDataTamp.put(index++, t);
                    }
                    return pageDataTamp;
                }));
            }

            for (int i = 0; i < concurrentReadNum; i++) {
                try {
                    cacheData.put(pageNum + i, completableFutures.get(i).get());
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
                cachePageDeque.addLast(pageNum + i);
            }

            while (true) {
                // todo 先简单的实现一个，完整的功能需要考虑更多，包括并发，多线程，缓存效率等因素
                if (cacheData.size() - 1 > concurrentReadNum) {
                    this.cacheData.remove(cachePageDeque.pollFirst());
                } else {
                    break;
                }
            }
        }
        return cacheData.get(pageNum);
    }

    /**
     * 计算index所在页码
     *
     * @param index
     * @return
     */
    private int calcPage(int index, int pageSize) {
        return index / pageSize + 1;
    }

    private int calcRelativeIndex(int index, int pageSize) {
        return index - ((int) (index / pageSize)) * pageSize;
    }

    @Override
    public Iterator<T> iterator() {
        return new Itr();
    }

    private class Itr implements Iterator<T> {

        private int index = 0;

        private boolean hasNext;

        private boolean isTriggerFirstAction = false;

        private boolean isTriggerLastAction = false;

        @Override
        public boolean hasNext() {
            if (index == 0) {
                if (!isTriggerFirstAction) {
                    isTriggerFirstAction = true;
                    dataPool.beforeFirstElementRead();
                }

                if (get(index) == null) {
                    hasNext = false;
                    if (!isTriggerLastAction) {
                        isTriggerLastAction = true;
                        dataPool.afterLastElementRead(calcPage(index, holdPageSize), holdPageSize, index);
                    }
                } else {
                    hasNext = true;
                }
            }

            return hasNext;
        }

        @Override
        public T next() {
            DataInfo<T> dataInfo = get(this.index);
            if (dataInfo == null) {
                throw new IndexOutOfBoundsException("Index: " + index);
            }

            hasNext = dataInfo.isHasNext();

            if (!hasNext && !isTriggerLastAction) {
                isTriggerLastAction = true;
                dataPool.afterLastElementRead(calcPage(index, holdPageSize), holdPageSize, index + 1);
            }

            this.index++;

            return dataInfo.getData();
        }
    }
}
