package com.briup.mapred.exercise;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/*
 * 统计专利被引用次数
 */
public class PatentCite extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PatentCite(), args);
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf=getConf();
		Job job=Job.getInstance(conf,"PatentCite");
		job.setJarByClass(PatentCite.class);
		
		job.setMapperClass(PCMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//装配Reducer
		job.setReducerClass(PCReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(conf.get("inpath")));
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));
		
		job.waitForCompletion(true);
		return 0;
	}
	
	

	private static class PCMap extends Mapper<LongWritable,Text,Text,IntWritable>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String line=value.toString();
			String[] nums=line.split(",");
			if(nums.length==2) {
				context.write(new Text(nums[1]), new IntWritable(1));
			}
		}
	}

	private static class PCReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int sum=0;
			for(IntWritable value:values) {
				sum+=value.get();
			}
			context.write(new Text(key), new IntWritable(sum));
		}
	}

}
