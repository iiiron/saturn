package net.noboard.saturn;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class MyDataPool implements DataPool<String> {

    private List<String> data;

    private String tip;

    public MyDataPool(String tip, int count) {
        this.tip = tip;
        data = new ArrayList<>();
        for (int i = 0; i <= count; i++) {
            data.add(i + tip);
        }
        System.out.println(data);
    }

    public List<String> getData() {
        return data;
    }

    @Override
    public Collection<String> read(int pageNum, int pageSize) {
        System.out.println("read " + tip + ", pageNum=" + pageNum + "pageSize=" + pageSize);
        int start = (pageNum - 1) * pageSize;
        List<String> result = new ArrayList<>();
        for (int i = 0; i < pageSize; i++) {
            if ((start + i) < data.size()) {
                result.add(data.get(start + i));
            }
        }
//        System.out.println(result);
        return result;
    }

    @Override
    public void afterLastElementRead(int pageNum, int pageSize, int count) {
        System.out.println(tip + " afterReadLastElement：pageNum=" + pageNum + ", pageSize=" + pageSize + ", count=" + count);
    }

    @Override
    public void beforeFirstElementRead() {
        System.out.println(tip + " beforeReadFirstElement");
    }
}
