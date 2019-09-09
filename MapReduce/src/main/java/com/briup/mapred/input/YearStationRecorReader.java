package com.briup.mapred.input;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import com.briup.mapred.base.YearStation;

public class YearStationRecorReader 
	extends RecordReader<YearStation, DoubleWritable>{
	private LineRecordReader reader = 
			new LineRecordReader();
	private YearStation ys = new YearStation();
	private DoubleWritable tmp_w = new DoubleWritable();
	@Override
	public void close() throws IOException {
		reader.close();
	}

	@Override
	public YearStation getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return ys;
	}

	@Override
	public DoubleWritable getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return tmp_w;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return reader.getProgress();
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		reader.initialize(arg0, arg1);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		Text value = null;
		String line = null;
		String stationid = null;
		String year = null;
		String tmp = null;
		String qua = null;
		//no more elements
		do{
			if(!reader.nextKeyValue())
				return false;
			//get current line and package my class
			value = reader.getCurrentValue();
			line = value.toString();
			stationid = line.substring(0, 15);
			year = line.substring(15,19);
			tmp = line.substring(87, 92);
			qua = line.substring(92, 93);
		}while(!qua.matches("[01459]")
				|| tmp.equals("9999"));
		//选取有效数据进行封装
		ys = new YearStation
				(year, stationid);
		double tmp_d = 
			Double.parseDouble(tmp);
		
		tmp_w = new DoubleWritable(tmp_d);
		
		return true;
	}

}




