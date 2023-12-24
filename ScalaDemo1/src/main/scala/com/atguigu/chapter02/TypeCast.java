package com.atguigu.chapter02;

public class TypeCast {
    public static void main(String[] args) {
        byte n = 23;
        test(n);
    }

    public static void test(byte b) {
        System.out.println("bbbb");
    }

    public static void test(short b) {
        System.out.println("ssss");
    }

    public static void test(char b) {
        System.out.println("cccc");
    }

    public static void test(int b) {
        System.out.println("iiii");
    }
}
