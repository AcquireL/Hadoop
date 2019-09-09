package com.briup.mapred.base;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
 * 复合数据类型的使用
 * 统计每年每个气象站的平均温度
 */
public class AvgTmpByYearStation extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new AvgTmpByYearStation(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf=getConf();
		
		Job job=Job.getInstance(conf,"avgTmpByYS");
		job.setJarByClass(this.getClass());
		
		job.setMapperClass(AvgMapper.class);
		job.setMapOutputKeyClass(YearStation.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setReducerClass(AvgReducer.class);
		job.setOutputKeyClass(YearStation.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(conf.get("inpath")));
		TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));
		
		return job.waitForCompletion(true)?0:1;
	}
	public static class AvgMapper extends Mapper<LongWritable,Text,YearStation,DoubleWritable>{
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, YearStation, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			//提取字段 Year Stationid tmp 数据质量 
			/*
			 （0， 15）气象站编号
			（15，19）年份
			（87， 92) 检查到的温度，如果为+9999则表示没有检测到温度
			（92， 93）温度数据质量，为[01459]表示该温度是合理温度
             */
			String line=value.toString();
			String stationid=line.substring(0, 15);
			String year=line.substring(15,19);
			String tmp=line.substring(87,92);
			String qua=line.substring(92,93);
			//选区有效数据进行封装 
			if(qua.matches("[01459]")&&!tmp.equals("9999")){
				YearStation ys=new YearStation(year,stationid);
				double tmp_d = Double.parseDouble(tmp);
				DoubleWritable tmp_w=new DoubleWritable(tmp_d);
				//输出数据
				context.write(ys, tmp_w);
			}
		}
	}
	public static class AvgReducer extends Reducer<YearStation,DoubleWritable,YearStation,DoubleWritable>{
		@Override
		protected void reduce(YearStation key, Iterable<DoubleWritable> values,
				Reducer<YearStation, DoubleWritable, YearStation, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			double sum=0.0;
			int num=0;
			for(DoubleWritable value:values) {
				sum+=value.get();
				num++;
			}
			double avgTmp=sum/num;
			context.write(key, new DoubleWritable(avgTmp));
		}
	}
}
