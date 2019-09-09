package com.briup.mapred.join.mapside;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MapSideJoinDriver extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new MapSideJoinDriver(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		//分区个数
		int numPartition=2;
		//定义路径
		Configuration conf=getConf();
		String firstIn1=conf.get("inpath1");
		String firstIn2=conf.get("inpath2");
		String firstOut1="/firstOut1";
		String firstOut2="/firstOut2";
		String out=conf.get("outpath");
		Job firstJob=Job.getInstance(conf,"first");
		firstJob.setJarByClass(FirstStage.class);
		//======处理arist.txt========
		firstJob.setMapperClass(FirstStage.SortByKeyMapper.class);
		firstJob.setMapOutputKeyClass(Text.class);
		firstJob.setMapOutputValueClass(Text.class);
		
		firstJob.setReducerClass(FirstStage.SortByKeyReducer.class);
		firstJob.setOutputKeyClass(NullWritable.class);
		firstJob.setOutputValueClass(Text.class);
		
		firstJob.setInputFormatClass(TextInputFormat.class);
		firstJob.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(firstJob,new Path(firstIn1));
		TextOutputFormat.setOutputPath(firstJob, new Path(firstOut1));
		
		firstJob.setNumReduceTasks(numPartition);
		TextOutputFormat.setOutputCompressorClass(firstJob,GzipCodec.class);
		
		//======处理user_arist.txt====
		Job secondJob=Job.getInstance(conf,"second");
		secondJob.setJarByClass(FirstStage.class);
		secondJob.setMapperClass(FirstStage.SortByKeyMapper.class);
		secondJob.setMapOutputKeyClass(Text.class);
		secondJob.setMapOutputValueClass(Text.class);
		
		secondJob.setReducerClass(FirstStage.SortByKeyReducer.class);
		secondJob.setOutputKeyClass(NullWritable.class);
		secondJob.setOutputValueClass(Text.class);
		
		secondJob.setInputFormatClass(TextInputFormat.class);
		secondJob.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(secondJob,new Path(firstIn2));
		TextOutputFormat.setOutputPath(secondJob, new Path(firstOut2));
		
		secondJob.setNumReduceTasks(numPartition);
		TextOutputFormat.setOutputCompressorClass(secondJob,GzipCodec.class);
		//======连接任务===========
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		          
		
		//生成连接表达式 inner outter
		String expr = CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class, new Path(firstOut1),new Path(firstOut2));
		//连接表达式分发给接收连接数据mapper任务
		System.out.println("======="+expr+"=======");
		conf.set("mapreduce.join.expr", expr);
		Job joinjob=Job.getInstance(conf,"joinjob");
		joinjob.setJarByClass(this.getClass());
		joinjob.setInputFormatClass(CompositeInputFormat.class);
		joinjob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(joinjob, new Path(firstOut1),new Path(firstOut2));
		TextOutputFormat.setOutputPath(joinjob, new Path(out));
		
		joinjob.setMapperClass(JoinMapper.class);
		joinjob.setMapOutputKeyClass(NullWritable.class);
		joinjob.setMapOutputValueClass(Text.class);
		joinjob.setNumReduceTasks(0);
		//========按顺序提交========
		List<Job> list=new ArrayList<>();
		list.add(firstJob);
		list.add(secondJob);
		list.add(joinjob);
		for(Job job:list) {
			boolean succ=job.waitForCompletion(true);
			if(!succ) {
				System.out.println("Error info:"+job.getJobName()+" "+job.getStatus().getFailureInfo());
				break;
			}
		}
		return 0;
	}
	public static class JoinMapper extends Mapper<Text,TupleWritable,NullWritable,Text>{
		@Override
		protected void map(Text key, TupleWritable value,
				Mapper<Text, TupleWritable, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			StringBuffer sb=new StringBuffer();
			sb.append(key.toString());
			for(Writable v:value) {
				sb.append(",");
				sb.append(v.toString());
			}
			context.write(NullWritable.get(), new Text(sb.toString()));
		}
	}
}
