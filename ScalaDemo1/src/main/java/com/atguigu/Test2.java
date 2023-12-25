package com.atguigu;

public class Test2 {
    public static void main(String[] args) {

//        Person p1 = new Person<Integer>();
//        Person p2 = new Person<Long>();

//        p1 = p2;

        // Java 泛型非变
        A<Person> a1 = new A<Person>();
        A<Student> a2 = new A<Student>();
        // a1 = a2; // 不可以赋值
    }
}

// java 泛型
class A<T>{

}

class Person {}

class Student extends Person{}
