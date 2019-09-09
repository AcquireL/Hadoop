package com.briup.knn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 *  Knn第三步：
 *  	把待识别数据向量和训练数据向量，进行求相似度运算，利用欧氏距离
 *       1.输入位置：hdfs文件系统 ；训练库数据：/knn_data/train_bin  待识别图片数据：/knn_data/unknown
 *       2.输出位置 hdfs文件系统：/knn_data/gsd_result  文件内容：（标签    相似度）
 */
public class GetSimilarityDegree extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new GetSimilarityDegree(), args);
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf=getConf();
		Job job=Job.getInstance(conf,"gsd");
		job.setJarByClass(this.getClass());
		job.setMapperClass(GSDMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		SequenceFileInputFormat.addInputPath(job, new Path("/knn_data/train_bin"));
		TextOutputFormat.setOutputPath(job, new Path("/knn_data/gsd_result"));
		job.waitForCompletion(true);
		return 0;
	}
	static char[] unknown = new char[400];
	static int sum;
	public static class GSDMapper extends Mapper<Text,Text,Text,DoubleWritable>{
		@Override
		protected void setup(Mapper<Text, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			//获取待识别向量  /knn_data/unkown
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream in = fs.open(new Path("/knn_data/unknown"));
			BufferedReader read=new BufferedReader(new InputStreamReader(in));
			unknown = read.readLine().toCharArray();
		}
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			//计算相识度
			char[] train_array = value.toString().toCharArray();
			for(int i=0;i<400;i++) {
				//待识别向量中，下标为i的值 某个维度
				int x=Integer.parseInt(Character.toString(unknown[i]));
				//训练集某个向量中，下标为i的值 某个维度
				int t=Integer.parseInt(Character.toString(train_array[i]));
				sum+=(x-t)*(x-t);
			}
			double degree=1/(1+Math.sqrt(sum));
			context.write(key, new DoubleWritable(degree));
		}
		
	}
	
}
