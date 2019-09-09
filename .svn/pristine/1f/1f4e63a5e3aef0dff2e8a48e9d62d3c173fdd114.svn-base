package com.briup.mapred.combiner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class TmpValue implements Writable{
	//温度
	private DoubleWritable tmp=new DoubleWritable();
	//权重，该温度是通过几个值算出来的
	private IntWritable num=new IntWritable();
	//装配job时候，大多数类型都是用xxx.class
	//MapReduce程序运行时，是利用了反射的技术
	//拿到了xxx这个类的对象（序列化反序列化）
	//但是要注意，反射中一般是利用newInstance（）产生对象的
	//这个方法等同于调用了xxx类中的无参构造器
	//所以，自定义类型的时候要显式定义无参构造器
	public TmpValue() {
	   
	}
	public TmpValue(DoubleWritable tmp,IntWritable num) {
		this.tmp=new DoubleWritable(tmp.get());
		this.num=new IntWritable(num.get());
	}
	public TmpValue(double tmp,int num) {
		this.tmp=new DoubleWritable(tmp);
		this.num=new IntWritable(num);
	}
	
	
	public DoubleWritable getTmp() {
		return tmp;
	}
	public void setTmp(DoubleWritable tmp) {
		this.tmp = new DoubleWritable(tmp.get());
	}
	public IntWritable getNum() {
		return num;
	}
	public void setNum(IntWritable num) {
		this.num = new IntWritable(num.get());
	}
	
	@Override
	public String toString() {
		return "TmpValue [tmp=" + tmp + ", num=" + num + "]";
	}
	@Override
	public void write(DataOutput out) throws IOException {
		tmp.write(out);
		num.write(out);
	}
/*	
 * public String Hello(String name) {
		System.out.println(name+"Hello");
		return name+":Hello";
	}
*/
	@Override
	public void readFields(DataInput in) throws IOException {
		tmp.readFields(in);
		num.readFields(in);
	}

}
