package net.noboard.saturn;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class SaturnTest {
    @Test
    public void test() {
        MyDataPool myDataPool = new MyDataPool("A", 1);
        MyDataPool myDataPool2 = new MyDataPool("B", 10);
        MyDataPool myDataPool3 = new MyDataPool("C", 100);

        List<String> result = new ArrayList<>();
        for (String s : Saturn.connect(10, myDataPool, myDataPool2, myDataPool3).forEach()) {
            result.add(s);
        }

        List<String> com = new ArrayList<>();
        com.addAll(myDataPool.getData());
        com.addAll(myDataPool2.getData());
        com.addAll(myDataPool3.getData());

        Assert.assertTrue(com.containsAll(result));
        Assert.assertTrue(result.containsAll(com));
        Assert.assertEquals(com.size(), result.size());
    }

    @Test
    public void test2() {
        MyDataPool myDataPool = new MyDataPool("A", -1);
        MyDataPool myDataPool2 = new MyDataPool("B", -1);
        MyDataPool myDataPool3 = new MyDataPool("C", -1);

        List<String> result = new ArrayList<>();
        for (List<String> s : Saturn.connect(7, myDataPool, myDataPool2, myDataPool3)
                .forEachBatchToList()) {
            result.addAll(s);
        }

        List<String> com = new ArrayList<>();
        com.addAll(myDataPool.getData());
        com.addAll(myDataPool2.getData());
        com.addAll(myDataPool3.getData());

        Assert.assertTrue(com.containsAll(result));
        Assert.assertTrue(result.containsAll(com));
        Assert.assertEquals(com.size(), result.size());
    }

    @Test
    public void test3() {
        int streamValve = 10;
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            int b = (int) (Math.random() * 100);
            if (b < streamValve) {
                list.add(b);
            }
        }

        System.out.println(list.size());

        MyDataPool myDataPool = new MyDataPool("A", 10);
        MyDataPool myDataPool2 = new MyDataPool("B", 9);
        MyDataPool myDataPool3 = new MyDataPool("C", 100);

        List<String> result = Saturn.connect(7, myDataPool, myDataPool2, myDataPool3)
                .getAllToList();

        List<String> com = new ArrayList<>();
        com.addAll(myDataPool.getData());
        com.addAll(myDataPool2.getData());
        com.addAll(myDataPool3.getData());

        Assert.assertTrue(com.containsAll(result));
        Assert.assertTrue(result.containsAll(com));
        Assert.assertEquals(com.size(), result.size());
    }

    @Test
    public void test4() {

        Saturn<Object> saturn = Saturn.connect(new DataPool<Object>() {
            @Override
            public Collection<Object> read(int pageNum, int pageSize) {
                return null;
            }

            @Override
            public void afterLastElementRead(int pageNum, int pageSize, int count) {
                System.out.println("Override afterLastElementRead");
            }

            @Override
            public void beforeFirstElementRead() {
                System.out.println("Override beforeFirstElementRead");
            }
        });

        saturn.forEach().forEach(o -> {});
    }
}