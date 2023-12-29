package com.atguigu;

import java.util.ArrayList;

/**
 * 1、 集合与数组的相同点是什么?
 * 	都是容器,可以存储多个数据
 * 2 、集合与数组的不同点是什么?
 * 	①.数组的长度是不可变的,集合的长度是可变的
 * 	②.数组可以存基本数据类型和引用数据类型
 * 	    集合只能存引用数据类型,如果要存基本数据类型,需要存对应的包装类
 */
public class JavaArrayDemo01 {
    public static void main(String[] args) {

        // 数组(array)：是一种用于存储多个相同数据类型的存储模型(可以理解为容器)
        // Java中的数组必须先初始化,然后才能使用
            //所谓初始化：就是为数组中的数组元素分配内存空间，并为每个数组元素赋值
            //注意：数组中的每一个数据，我们称之为数组中的元素
        // 一维数组声明
        //一维数组的声明方式：type var[] 或 type[] var；

        // 动态初始化：数组声明且为数组元素分配空间与赋值的操作分开进行
        int[] arr = new int[3];
        arr[0] = 3;
        arr[1] = 9;
        arr[2] = 8;
        System.out.println(arr);
        for (int i = 0; i < arr.length; i++) {
            System.out.println(arr[i]);
        }

        // 静态初始化：在定义数组的同时就为数组元素分配空间并赋值。
        int[] a = new int[]{ 3, 9, 8};
        System.out.println(a);

        // Java 定义集合
        ArrayList<Integer> integers = new ArrayList<>();
        integers.add(1);
        //integers.add(true);
        System.out.println(integers);

        // Collection集合的常用方法
        //     boolean add(E e)                    向集合中添加元素
        //     boolean remove(E e)              将元素从集合中删除
        //     boolean removeIf(Object o)   根据条件进行删除
        //     void clear()                              清空集合所有的元素
        //     boolean contains(E e)             判断集合中是否包含指定的元素
        //     boolean isEmpty()                   判断集合是否为空
        //     int size()                                   获取集合的长度
        System.out.println(integers.isEmpty());
        System.out.println(integers.size());

    }
}
