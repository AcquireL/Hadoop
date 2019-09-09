package com.briup.mapred.sort;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.RandomSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TotalSort extends 
	Configured implements Tool{
	public static void main(String[] args)
			throws Exception {
		ToolRunner.run(new TotalSort(), args);
	}
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		//ts_pre_result
		Path in = new Path(conf.get("inpath"));
		Path out = new Path(conf.get("outpath"));
		
		Job job = Job.getInstance
			(conf, "total_sort");
		job.setJarByClass(TotalSort.class);
		//切换输入格式
		job.setInputFormatClass
			(SequenceFileInputFormat.class);
		//输出格式还是Text
		job.setOutputFormatClass
			(TextOutputFormat.class);
		//默认mapper
		job.setMapOutputKeyClass
			(DoubleWritable.class);
		job.setMapOutputValueClass
			(Text.class);
		//默认reducer
		job.setOutputKeyClass
			(DoubleWritable.class);
		job.setOutputValueClass
			(Text.class);
		SequenceFileInputFormat
			.addInputPath(job, in);
		TextOutputFormat.
			setOutputPath(job, out);
		//------------------------------
		job.setPartitionerClass
			(TotalOrderPartitioner.class);
		job.setNumReduceTasks(2);
		
		RandomSampler<DoubleWritable, Text> 
		sampler = 
			new InputSampler.RandomSampler
				<DoubleWritable,Text>
					(0.5,200,5);
		//运行采样器，获得样本数据，并保存
		InputSampler.writePartitionFile
			(job, sampler);
		
		String partitionFile = 
			TotalOrderPartitioner.
			getPartitionFile(conf);
		URI uri = new URI(partitionFile);
		System.out.println("========"+uri.toString()+"========");
		//把保存到本地的分区文件，分发给job的各个节点
		job.addCacheFile(uri);
		
		job.waitForCompletion(true);
		return 0;
	}
}




