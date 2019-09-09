package com.briup.mapred.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class YearStat 
	implements DBWritable,WritableComparable<YearStat>{
	private IntWritable year = new IntWritable();
	private Text stationId = new Text(); 
	private DoubleWritable tmp = new DoubleWritable();
	public YearStat() {
	}
	public YearStat(String year,
			String stationid,String tmp) {
		int y = Integer.parseInt(year);
		this.year = new IntWritable(y);
		this.stationId = new Text(stationid);
		double t = Double.parseDouble(tmp);
		this.tmp = new DoubleWritable(t);
	}
	public YearStat
		(IntWritable year,Text stationId) {
		this.year = new IntWritable(year.get());
		this.stationId = 
			new Text(stationId.toString());
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
	public DoubleWritable getTmp() {
		return tmp;
	}
	public void setTmp(DoubleWritable tmp) {
		this.tmp = new DoubleWritable(tmp.get());
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.year.readFields(in);
		this.stationId.readFields(in);
		this.tmp.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.year.write(out);
		this.stationId.write(out);
		this.tmp.write(out);
	}

	@Override
	public int compareTo(YearStat o) {
		return 
			(this.year.get()-o.year.get())==0
			?this.stationId.compareTo(o.stationId)
			:this.year.get()-o.year.get();
	}
	
	@Override
	public void readFields(ResultSet rs) throws SQLException {
		if(rs == null)
			return;
		this.year = new IntWritable(rs.getInt(1));
		this.stationId = new Text(rs.getString(2));
		this.tmp = new DoubleWritable(rs.getDouble(3));
	}
	@Override
	public void write(PreparedStatement prep) throws SQLException {
		prep.setInt(1, this.year.get());
		prep.setString(2, this.stationId.toString());
		prep.setDouble(3, this.tmp.get());
	}
	
}
