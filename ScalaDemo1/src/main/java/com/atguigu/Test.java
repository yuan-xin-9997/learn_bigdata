package com.atguigu;

public class Test {
    public static void main(String[] args) {
        m1(1,2,3,4,5,6);

        int[] xs = {1,2,3,4,5,6};
        System.out.println("-----------------------");
        m1(xs);
        System.out.println("-----------------------");

        // java匿名子类
        Function func = new Function() {
            @Override
            public int add(int x, int y) {
                return x + y;
            }
        };

        System.out.println(func);
        System.out.println(func.add(1, 3));

        Function1 func1 = new Function1<Integer, Integer, Integer>() {
            @Override
            public Integer apply(Integer v1, Integer v2) {
                return v1 + v2;
            }
        };
        System.out.println(func1);
        System.out.println(func1.apply(1, 2));
    }

    // java可变长参数方法
    public static void m1(int... xs){
        for (int i : xs) {
            System.out.println(i);
        }
    }
}

interface Function{
    int add(int x, int y);
}

// Java 泛型
interface Function1<T1, T2, T3>{
    T3 apply(T1 v1, T2 v2);
}