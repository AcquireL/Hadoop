package com.briup.mapred.base;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/*
 * 去重   去除a-b和b-a的相同个数
 */
public class DupMR extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new DupMR(), args);
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf=getConf();
		Job job=Job.getInstance(conf,"DupMR");
		job.setJarByClass(DupMR.class);
		
		job.setMapperClass(DupMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setReducerClass(DupReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(conf.get("inpath")));
		TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));
		
		return job.waitForCompletion(true)?0:1;
	}
	public static class DupMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String line=value.toString();
			String[] split = line.split(",");
			if(split.length==2&&split[0].trim().length()>0&&split[1].trim().length()>0) {
				String f1=split[0].trim();
				String f2=split[1].trim();
				String str_order=getOrder(f1,f2);
				context.write(new Text(str_order), NullWritable.get());
			}
		}

		private String getOrder(String f1, String f2) {
			if(f1.compareTo(f2)>0) {
				return f2+","+f1;
			}
			return f1+","+f2;
		}
	}
	public static class DupReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
}
