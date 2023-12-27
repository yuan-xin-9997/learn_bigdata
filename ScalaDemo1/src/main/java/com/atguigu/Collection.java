package com.atguigu;

import java.util.ArrayList;
import java.util.LinkedList;



public class Collection {
    /**
     * Java数据类型，集合
     * @param args
     */
    public static void main(String[] args) {

        // 集合Collection
        // 集合和数组的区别
            // 1，数组的长度是不可变的，集合的长度是可变的。
            //2，数组可以存基本数据类型和引用数据类型。
            //     集合只能存引用数据类型，如果要存基本数据类型，需要存对应的包装类。

        // Collection集合概述
        //是单例集合的顶层接口，它表示一组对象，这些对象也称为Collection的元素
        //JDK 不提供此接口的任何直接实现，它提供更具体的子接口（如Set和List）实现
        //
        //创建Collection集合的对象
        //多态的方式
        //具体的实现类ArrayList

        // 集合-Collection-List-ArrayList
        ArrayList<Object> arrayList = new ArrayList<>();
        arrayList.add(1);
        System.out.println(arrayList.isEmpty());
        arrayList.remove(new Integer(1));
        System.out.println(arrayList.isEmpty());
        System.out.println(arrayList.contains(new Integer(1)));
        arrayList.add(1);
        arrayList.add(2);
        arrayList.add(3);
        System.out.println(arrayList);
        System.out.println(arrayList.size());

        // 集合-Collection-List-LinkedList
        LinkedList<Boolean> linkedList = new LinkedList<>();
        linkedList.add(true);
        linkedList.add(false);
        System.out.println(linkedList);
        // linkedList.add(1);  // 报错

        // Collection-List
        // List集合概述
        //有序集合，这里的有序指的是存取顺序
        //用户可以精确控制列表（list集合）中每个元素的插入位置。用户可以通过整数索引访问元素，并搜索列表中的元素
        //与Set集合不同，列表通常允许重复的元素
        //
        //List集合特点
        //有序：存储和取出的元素顺序一致
        //有索引：可以通过索引操作元素
        //可重复：存储的元素可以重复

        // List集合常用子类：ArrayList，LinkedList
        //ArrayList：底层数据结构是数组，查询快，增删慢
        //LinkedList：底层数据结构是链表，查询慢，增删快
        //
        //练习：
        //       使用LinkedList完成存储字符串并遍历


        // Collection-Set
        // Set集合特点
        //可以去除重复
        //存取顺序不一致
        //没有带索引的方法，所以不能使用普通for循环遍历，也不能通过索引来获取、删除Set集合里面的元素
        //
        //Set集合练习
        //存储字符串并遍历

        // TreeSet集合特点
        //不包含重复元素的集合
        //没有带索引的方法
        //可以将元素按照规则进行排序
        //
        //
        //TreeSet集合练习
        //存储Integer类型的整数，并遍历
        //存储学生对象，并遍历

        // HashSet集合特点
        //底层数据结构是哈希表
        //对集合的迭代顺序不作任何保证，也就是说不保证存储和取出的元素顺序一致
        //没有带索引的方法，所以不能使用普通for循环遍历
        //由于是Set集合，所以元素唯一


        // Collection-Map
        // Interface Map<K,V>	K：键的数据类型；V：值的数据类型
        //键不能重复，值可以重复
        //键和值是一一对应的，每一个键只能找到自己对应的值
        //（键 + 值） 这个整体 我们称之为“键值对”或者“键值对对象”，在Java中叫做“Entry对象”。

        // HashMap底层是哈希表结构的
        //依赖hashCode方法和equals方法保证键的唯一
        //如果键要存储的是自定义对象，需要重写hashCode和equals方法

        // TreeMap是Map里面的一个实现类。
        //没有额外需要学习的特有方法，直接使用Map里面的方法就可以了
        //TreeMap跟TreeSet一样底层是红黑树结构的
        // TreeMap底层是红黑树结构的
        //依赖自然排序或者比较器排序，对键进行排序
        //如果键存储的是自定义对象，需要实现Comparable接口或者在创建TreeMap对象时候给出比较器排序规则
    }
}
