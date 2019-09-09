package com.briup.mapred.chain;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class PatentRefCount extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PatentRefCount(), args);
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf=getConf();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		//注意
		//1 先设置conf属性值，在获取job
		//2 先获取job，在拿出job中的conf设置的属性值
		Job job=Job.getInstance(conf,"chain_test");
		job.setJarByClass(this.getClass());
		//将输入数据进行分割
		//1.装配InverseMapper进行id Refid 的交换
		ChainMapper.addMapper(job, InverseMapper.class, Text.class, Text.class,  Text.class,  Text.class, conf);
		
		//2.整理出refid 1 进行输出
		ChainMapper.addMapper(job, MyMapper.class, Text.class, Text.class, Text.class, LongWritable.class, conf);
		
		//3.装配longSunReducer进行计数值的累加
		ChainReducer.setReducer(job, LongSumReducer.class, Text.class, LongWritable.class, Text.class, LongWritable.class, conf);
		//4 追加Mapper 测试Reducer后能不能再跟Mapper
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		KeyValueTextInputFormat.addInputPath(job, new Path(conf.get("inpath")));
		TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));
		
		return job.waitForCompletion(true)?0:1;
	}
	public static class MyMapper extends Mapper<Text,Text,Text,LongWritable>{
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(key, new LongWritable(1));
		}
	}
}
