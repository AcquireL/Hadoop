package com.briup.mapred.combiner;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;

//反射
public class ReflectTest {
	static String name=" ";
	static {
		Properties prop=new Properties();
		try {
			prop.load(new FileInputStream("src/main/resources/info.properties"));
			name = prop.getProperty("classname");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	@SuppressWarnings("rawtypes")
	public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		
		Class cla = Class.forName(name);
		Object obj=cla.newInstance();
		Method[] methods = cla.getDeclaredMethods();
		Method m=methods[2];
		System.out.println(m.getName()+" "+m.getReturnType());
		for(Class ps:m.getParameterTypes()) {
			System.out.println(ps.getName());
		} 
		Object r = m.invoke(obj,new Object[] {"jack"} );
		System.out.println(r);
		System.out.println("--------------");
	}
}
