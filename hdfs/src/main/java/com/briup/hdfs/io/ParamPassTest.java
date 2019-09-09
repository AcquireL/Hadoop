package com.briup.hdfs.io;

public class ParamPassTest {
	public static void main(String[] args) {
		int x=10;
		int y=10;
		test(x, y);
		System.out.println("main:"+x+" "+y);
		Teacher tea1=new Teacher(1, "jack", 1000);
		Teacher tea2=new Teacher(2, "rose", 2000);
		test2(tea1, tea2);
		System.out.println("main:"+tea1+" "+tea2);
	}
	public static void test(int a,int b) {
		a++;
		b++;
		System.out.println("test1:"+a+" "+b);
	}
	public static void test2(Teacher t1,Teacher t2) {
		t1.setSalary(8888);
		t2.setSalary(9999);
		System.out.println("test2:"+t1+" "+t2);
	}
}
