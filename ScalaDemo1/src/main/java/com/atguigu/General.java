package com.atguigu;

import java.sql.Array;
import java.util.ArrayList;
import java.util.LinkedList;

public class General {
    /**
     * Java 泛型、数组、集合、反射
     * @param args
     */
    public static void main(String[] args) {
        // Java 泛型
        // 泛型：是JDK5中引入的特性，它提供了编译时类型安全检测机制
        //
        //泛型的好处：
        //把运行时期的问题提前到了编译期间
        //避免了强制类型转换

        // 型可以使用的地方:
        //	类后面  ==>  泛型类
        //	方法申明上  ==>  泛型方法
        //	接口后面  ==>   泛型接口

        // 如果一个类的后面有<E>，表示这个类是一个泛型类。
        //创建泛型类的对象时，必须要给这个泛型确定具体的数据类型。

        // 泛型的定义格式：
        //<类型>：指定一种类型的格式。
        //尖括号里面可以任意书写，按照变量的定义规则即可。一般只写一个字母。
        //	比如：<E>  <T> <Q> <M>
        //<类型1,类型2…>：指定多种类型的格式，多种类型之间用逗号隔开。
        //比如：<E , T>  <Q , M> <K , V>

        // 泛型类的定义格式：
        //格式：修饰符 class 类名<类型> {  }
        //范例：public class Generic<T> {  }
        //                 此处T可以随便写为任意标识，常见的如T、E、K、V等形式的参数常用于表示泛型

        // 泛型方法的定义格式：
        //格式：修饰符 <类型> 返回值类型 方法名(类型 变量名) {  }
        //范例：public <T> void show(T t) {  }

        // 泛型接口的使用方式：
        //实现类也不给泛型
        //实现类确定具体的数据类型
        // 泛型接口的定义格式：
        //格式：修饰符 interface 接口名<类型> {  }
        //范例：public interface Generic<T> {  }

        // 类型通配符：<?>
        //ArrayList<?>：表示元素类型未知的ArrayList，它的元素可以匹配任何的类型
        //但是并不能把元素添加到ArrayListList中了，获取出来的也是父类类型
        //
        //类型通配符上限：<? extends 类型>
        //比如： ArrayListList <? extends Number>：它表示的类型是Number或者其子类型
        //
        //类型通配符下限：<? super 类型>
        //比如： ArrayListList <? super Number>：它表示的类型是Number或者其父类型


        // Java 数组
        // Java中的数组必须先初始化,然后才能使用
        //所谓初始化：就是为数组中的数组元素分配内存空间，并为每个数组元素赋值
        //注意：数组中的每一个数据，我们称之为数组中的元素
        // Java数组可以保存，基本数据类型、引用数据类型
        // Java 数组长度不可以变，元素可变
        int[] ints = {1, 2, 3};
        System.out.println(ints);

        Integer e1 = new Integer(1);
        Integer e2 = new Integer(2);
        Integer e3 = new Integer(2);
        Integer[] integers = {e1, e2, e3};
        System.out.println(integers);

        Person1 person1 = new Person1();
        Person1[] person1s = {person1};
        System.out.println(person1s);
        ints[0] = 2;
        for (int anInt : ints) {
            System.out.println(anInt);
        }

        // 	JAVA推出泛型以前，程序员可以构建一个元素类型为Object的集合，该集合能够存储任意的数据类型对象，而在使用该集合的过程中，需要程序员明确知
        // 	道存储每个元素的数据类型，否则很容易引发ClassCastException异常。
        //         Java泛型（generics）是JDK5中引入的一个新特性，泛型提供了编译时类型安全监测机制，该机制允许我们在编译时检测到非法的类型数据结构。
        //
        //        泛型的本质就是参数化类型，也就是所操作的数据类型被指定为一个参数。

        InfoImpl info = new InfoImpl();
        System.out.println(info.info(1));


    }
}


class Person1 {
    private String name;
    private int age;
}

interface Info<T>{
    T info(T var);
}

class InfoImpl implements Info<Integer>{

    @Override
    public Integer info(Integer var) {
        return var;
    }
}