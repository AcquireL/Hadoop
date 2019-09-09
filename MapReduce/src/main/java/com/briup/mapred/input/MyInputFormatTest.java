package com.briup.mapred.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.briup.mapred.base.YearStation;


public class MyInputFormatTest extends Configured implements Tool{
	public static class MaxTmpReducer 
		extends Reducer<YearStation, DoubleWritable, 
			YearStation, DoubleWritable>{
	@Override
	protected void reduce(YearStation key, 
		Iterable<DoubleWritable> values,
			Reducer<YearStation, DoubleWritable,
			YearStation, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		double max = 0.0;
		for (DoubleWritable value : values) {
			max = Math.max(max, value.get());
		}
		context.write(key, new DoubleWritable(max));
	}
}
	public static void main(String[] args)
			throws Exception {
		ToolRunner.run(new MyInputFormatTest(), args);
	}
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf,"MyInputTest");
		job.setJarByClass(this.getClass());
		
		job.setMapperClass(Mapper.class);
		job.setMapOutputKeyClass(YearStation.class);
		job.setMapOutputValueClass
			(DoubleWritable.class);
		
		job.setReducerClass(MaxTmpReducer.class);
		job.setOutputKeyClass(YearStation.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setInputFormatClass
			(YearStationInputFormat.class);
		job.setOutputFormatClass
			(TextOutputFormat.class);
		YearStationInputFormat.addInputPath
			(job, new Path(conf.get("inpath")));
		TextOutputFormat.setOutputPath
			(job, new Path(conf.get("outpath")));
		return job.waitForCompletion(true)?0:1;
	}
}
