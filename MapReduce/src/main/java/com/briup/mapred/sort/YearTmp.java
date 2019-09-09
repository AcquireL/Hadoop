package com.briup.mapred.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

//自然键和自然值构建复合键
// 完成排序
public class YearTmp 
	implements WritableComparable<YearTmp>{
	private IntWritable year = new IntWritable();
	private DoubleWritable tmp = new DoubleWritable();
	public YearTmp() {
	}
	
	public YearTmp(IntWritable year, DoubleWritable tmp) {
		this.year = new IntWritable(year.get());
		this.tmp = new DoubleWritable(tmp.get());
	}
	public YearTmp(int year, double tmp) {
		this.year = new IntWritable(year);
		this.tmp = new DoubleWritable(tmp);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year.readFields(in);
		this.tmp.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.year.write(out);
		this.tmp.write(out);
	}

	//友元
	@Override
	public int compareTo(YearTmp o) {
		return this.year.compareTo(o.year)==0?
			   this.tmp.compareTo(o.tmp):
			   this.year.compareTo(o.year);
	}
	@Override
	public String toString() {
		return this.year.get()+"\t"+this.tmp.get();
	}

	public IntWritable getYear() {
		return year;
	}

	public void setYear(IntWritable year) {
		this.year = new IntWritable(year.get());
	}

	public DoubleWritable getTmp() {
		return tmp;
	}

	public void setTmp(DoubleWritable tmp) {
		this.tmp = new DoubleWritable(tmp.get());
	}
	
	
}

