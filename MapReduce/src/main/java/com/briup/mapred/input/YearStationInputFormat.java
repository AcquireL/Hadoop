package com.briup.mapred.input;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.briup.mapred.base.YearStation;

public class YearStationInputFormat 
	extends FileInputFormat
		<YearStation, DoubleWritable>{

	@Override
	public RecordReader<YearStation, DoubleWritable> 
		createRecordReader(InputSplit split,
				TaskAttemptContext context)
			throws IOException, InterruptedException {
		YearStationRecorReader reader = 
			new YearStationRecorReader();
		reader.initialize(split, context);
		return reader;
	}
}




