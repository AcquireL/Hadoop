package com.briup.mapred.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class YearStation  implements WritableComparable<YearStation> {

	private IntWritable year=new IntWritable();
	private Text stationId=new Text();
	
	
	public YearStation() {
	}

	public YearStation(String year,String stationid) {
		int y=Integer.parseInt(year);
		this.year=new IntWritable(y);
		this.stationId=new Text(stationid);
	}
	public YearStation(IntWritable year,Text stationId) {
		this.year=new IntWritable(year.get());
		this.stationId=new Text(stationId.toString());
	}
	
	public IntWritable getYear() {
		return year;
	}

	public void setYear(IntWritable year) {
		this.year = new IntWritable(year.get());
	}

	public Text getStationId() {
		return stationId;
	}

	public void setStationId(Text stationId) {
		this.stationId = new Text(stationId.toString());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.year.write(out);
		this.stationId.write(out);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year.readFields(in);
		this.stationId.readFields(in);
		
	}

	@Override
	public int compareTo(YearStation o) {
		return (this.year.get()-o.year.get()==0)?this.stationId.compareTo(o.stationId):this.year.get()-o.year.get();
	}

	@Override
	public String toString() {
		return this.year.get()+"\t"+this.stationId.toString();
	}
	

}
