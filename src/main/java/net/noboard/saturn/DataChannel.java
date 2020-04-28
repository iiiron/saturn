package net.noboard.saturn;

import java.util.*;


public class DataChannel<T> {

    private DataPool<T> dataPool;

    /**
     * 默认分页长度
     */
    private final static int DEFAULT_HOLD_PAGE_SIZE = 100;

    /**
     * 持有数据总页数
     */
    private final static int HOLD_PAGES = 2;

    /**
     * 分页长度
     */
    private int holdPageSize;

    private Map<Integer, Map<Integer, T>> cacheData;

    private ArrayDeque<Integer> cachePageDeque;

    private DataChannel(DataPool<T> dataPool, int pageSize) {
        this.dataPool = dataPool;
        this.cacheData = new HashMap<>();
        this.cachePageDeque = new ArrayDeque<>();
        this.holdPageSize = pageSize;
    }

    public DataReader<T> reader() {
        return new DataReader<T>(this);
    }

    public static <T> DataChannel<T> connect(DataPool<T> dataPool, int pageSize) {
        return new DataChannel<>(dataPool, pageSize);
    }

    public static <T> DataChannel<T> connect(DataPool<T> dataPool) {
        return new DataChannel<>(dataPool, DEFAULT_HOLD_PAGE_SIZE);
    }

    synchronized DataInfo<T> get(int index) {
        if (index == 0) {
            dataPool.beforeFirstReadElement();
        }

        int pageNum = calcPage(index, holdPageSize);

        Map<Integer, T> pageData = loadPageData(pageNum);
        if (pageData == null) {
            dataPool.afterLastReadElement(pageNum, holdPageSize, index);
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

        if (!hasNext) {
            dataPool.afterLastReadElement(pageNum, holdPageSize, index + 1);
        }

        return new DataInfo<>(pageData.get(index), hasNext);
    }

    private Map<Integer, T> loadPageData(int pageNum) {
        Map<Integer, T> pageData = cacheData.get(pageNum);
        if (pageData == null) {
            Collection<T> data = dataPool.read(pageNum, holdPageSize);
            if (data == null || data.size() == 0) {
                return null;
            }

            pageData = new HashMap<>();
            int i = (pageNum - 1) * holdPageSize;
            for (T t : data) {
                pageData.put(i++, t);
            }

            cacheData.put(pageNum, pageData);

            cachePageDeque.addLast(pageNum);

            while (true) {
                // todo 先简单的实现一个，完整的功能需要考虑更多，包括并发，多线程，缓存效率等因素
                if (cacheData.size() > HOLD_PAGES) {
                    this.cacheData.remove(cachePageDeque.pollFirst());
                } else {
                    break;
                }
            }
        }
        return pageData;
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
}
