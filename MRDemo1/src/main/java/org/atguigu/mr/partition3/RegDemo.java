package org.atguigu.mr.partition3;

public class RegDemo {
    public static void main(String[] args) {
        String s = "abc";
        boolean flag = s.matches("^[a-p,A-p]*");
        System.out.println(flag);
    }
}
