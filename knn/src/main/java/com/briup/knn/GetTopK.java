package com.briup.knn;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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

/*
 * Knn第五步：
 * 	  选取最相似的K个，根据标签统计个数，且计算平均相似度（10个 20个都行，根据数据规模定，目前训练数据有6w多条，所以k值建议取 50-100个）
	  数据输出内容：	（标签   平均相似度   个数  ）
	   1.输入位置：hdfs文件系统  /knn_data/gsd_result_sorted
	   2.输出位置：hdfs文件系统 /knn_data/gtk_result
	 注意：map按原样输出时记得设置分组比较器
 */
public class GetTopK extends 
	Configured implements Tool{
	public static void main(String[] args)
			throws Exception {
		ToolRunner.run(new GetTopK(), args);
	}
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf =getConf();
		Job job = Job.getInstance(conf, "gtk");
		job.setJarByClass(this.getClass());
		job.setMapperClass(GTKMapper.class);
		job.setMapOutputKeyClass(TagDegree.class);
		job.setMapOutputValueClass
			(Text.class);
		job.setReducerClass(GTKReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass
			(TextInputFormat.class);
		job.setOutputFormatClass
			(TextOutputFormat.class);
		TextInputFormat.
			addInputPath(job, 
				new Path("/knn_data/gsd_result_sorted"));
		TextOutputFormat.
			setOutputPath(job, 
				new Path("/knn_data/gtk_result"));
		//设置分组比较器
		job.setGroupingComparatorClass
			(TagDegreeGroupComparator.class);
		job.waitForCompletion(true);
		return 0;
	}
	
	public static class GTKMapper 
		extends Mapper<LongWritable, Text, 
		TagDegree, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, TagDegree, Text>.Context context)
				throws IOException, InterruptedException {
			String[] infos = value.toString().
					split("\t");
			String tag = infos[0]
				.substring(0, 1);
			Double degree = 
				Double.parseDouble(infos[1]);
			//TagDegree必须设置分组比较器，要不然每一行都会分一个组，导致小型文件过多，运行速度减慢
			TagDegree td = 
				new TagDegree(tag,degree);
			context.write(td, new Text("1"));
		}
	}
	public static class GTKReducer 
		extends Reducer<TagDegree, Text, 
		Text, Text>{
		@Override
		protected void reduce(TagDegree key, 
			Iterable<Text> values, 
			Reducer<TagDegree, Text, 
			Text, Text>.Context context)
				throws IOException, InterruptedException {
			//1 取前20个数据
			//2 计算平均相似度
			int i = 0;
			Map<String,AvgNum> map = new HashMap();
			Iterator<Text> ite = values.iterator();
			while(i < 30) {
				//遍历value的时候，同步更新key的数据
				Text next = ite.next();
				String tag = key.getTag().toString();
				double degree = key.getDegree().get();
				if(!map.containsKey(tag)) {
					AvgNum an = 
						new AvgNum(1, degree);
					map.put(tag, an);
				}else {
					AvgNum old_an = map.get(tag);
					double old_avg = old_an.getAvg();      
					int old_num = old_an.getNum();
					int new_num = old_num+1;
					double new_avg =
						(old_avg*old_num+degree)/new_num;
					AvgNum new_an = 
						new AvgNum(new_num, new_avg);
					map.put(tag, new_an);
				}
				i++;
			}
			for(Map.Entry<String, AvgNum> en:
				map.entrySet()) {
				context.write(
					new Text(en.getKey()),
					new Text(en.getValue().getAvg()+
						"\t"+en.getValue().getNum()));
			}
		}
	}
	
}
