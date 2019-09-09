package com.briup.mapred.base;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FlatFriends extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new FlatFriends(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf=getConf();
		Job job = Job.getInstance(conf,"flatFriends");
		job.setJarByClass(FlatFriends.class);
		//为job装配mapper
		job.setMapperClass(FFMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//为job装配reducer
		job.setReducerClass(FFReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(conf.get("inpath")));
		TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));
		
		return job.waitForCompletion(true)?0:1;
	}

	public static class FFMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split(",");
			if (split.length == 2 && split[0].trim().length() >= 1 && split[1].trim().length() >= 1) {
				context.write(new Text(split[0].trim()), new Text(split[1].trim()));
			}
		}
	}

	public static class FFReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			StringBuffer sb=new StringBuffer();
			for(Text value:values) {
				sb.append(","+value);
			}
			context.write(key, new Text(sb.toString()));
		}
	}

}
