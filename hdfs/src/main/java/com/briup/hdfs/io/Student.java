package com.briup.hdfs.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//java bean POJO��plain old java object
public class Student implements WritableComparable<Object> {
	private IntWritable id = new IntWritable();
	private Text name = new Text();
	private DoubleWritable weight = new DoubleWritable();

	public Student() {
	}

	public void write(DataOutput out) throws IOException {
		/*
		 * IntWritable id_w=new IntWritable(id); Text name_w =new Text(name);
		 * DoubleWritable weight_w=new DoubleWritable(weight); id_w.write(out);
		 * name_w.write(out); weight_w.write(out);
		 */
		// Student 
		getId().write(out);
		name.write(out);
		weight.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		getId().readFields(in);
		name.readFields(in);
		weight.readFields(in);
	}

	public IntWritable getId() {
		return id;
	}

	public void setId(IntWritable id) {
		this.id = new IntWritable(id.get());
	}

	public Text getName() {
		return name;
	}

	public void setName(Text name) {
		this.name = new Text(name.toString());
	}

	public DoubleWritable getWeight() {
		return weight;
	}

	public void setWeight(DoubleWritable weight) {
		this.weight = new DoubleWritable(weight.get());
	}

	@Override
	public int compareTo(Object o) {
		Student s = (Student) o;
		int result = (int) (this.weight.get() * 10 - s.weight.get()) * 10;
		return result;
	}

	public static class StudentComparator extends WritableComparator {
		public StudentComparator(Student s) {
			super(s.getClass());
		}
		/*
		 * @Override public int compare(WritableComparable a, WritableComparable b) {
		 * return super.compare(a, b); }
		 */

	}

}
