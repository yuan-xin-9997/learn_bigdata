package com.atguigu;

public class JavaSingletonDemo1 {

    /**
     * Java 实现单例
     * @param args
     */
    public static void main(String[] args) {
        PersonSingleton p1 = PersonSingleton.getInstance();
        PersonSingleton p2 = PersonSingleton.getInstance();
        System.out.println(p1);
        System.out.println(p2);
//        PersonSingleton p3 = new PersonSingleton(); // 'PersonSingleton()' has private access in 'com.atguigu.PersonSingleton'
    }
}


class PersonSingleton {
     // 私有的自有属性
    private static PersonSingleton p = new PersonSingleton();

    // 构造器私有
    private PersonSingleton(){}

    // 提供公有静态的获取对象的方法
    public static PersonSingleton getInstance(){
        return p;
    }
}
