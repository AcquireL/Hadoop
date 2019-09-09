package com.briup.mapred.base;

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
 * 统计词频：统计文章中所有单词的出现次数
 */
public class WordsCount extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new WordsCount(), args);
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf=getConf();
		Job job = Job.getInstance(conf,"wordcount");
		job.setJarByClass(WordsCount.class);
		//为job装配mapper
		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//为job装配reducer
		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//为job指定输入路径
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(conf.get("inpath")));
		//为job指定输出路径
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));
		//提交运行
		job.waitForCompletion(true);
		return 0;
	}
	public static class WCMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			//Text是包装类型，通过toString拿到实际数据
			String line = value.toString();
			String[] words=line.split(" ");
			for(String word:words) {
				context.write(new Text(word), new IntWritable(1));
			}
		}
	}
	public static class WCReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int sum=0;
			for (IntWritable value : values) {
				sum+=value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
}  //class


