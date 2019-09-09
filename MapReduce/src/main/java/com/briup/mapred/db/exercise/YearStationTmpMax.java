package com.briup.mapred.db.exercise;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/*
 * 输入数据：/data/tmp_data
 * 
 * 
 */
public class YearStationTmpMax extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new YearStationTmpMax(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "YearStationImpMax");
		job.setJarByClass(this.getClass());

		job.setMapperClass(YSTMapper.class);
		job.setMapOutputKeyClass(YearStationTmp.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setCombinerClass(YSTCombiner.class);
		job.setReducerClass(YSTReducer.class);
		job.setOutputKeyClass(YearStationTmp.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(conf.get("inpath")));
	/*	
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));
		*/
		
		
		job.setOutputFormatClass(DBOutputFormat.class);
		/*DBConfiguration.configureDB(job.getConfiguration(),"com.mysql.jdbc.Driver" ,"jdbc:mysql://192.168.0.103:3306/hadoop", "root", "713181");
		DBOutputFormat.setOutput(job, "maxtmp", "year","stationId","tmp");*/

		DBConfiguration.configureDB(job.getConfiguration(),"oracle.jdbc.driver.OracleDriver" ,"jdbc:oracle:thin:@192.168.0.103:1521:XE","briup","713181");
		DBOutputFormat.setOutput(job, "maxtmp", "year","stationId","tmp");
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class YSTMapper extends Mapper<LongWritable, Text, YearStationTmp, DoubleWritable> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, YearStationTmp, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String stationid = line.substring(0, 15);
			String year = line.substring(15, 19);
			String tmp = line.substring(87, 92);
			String qua = line.substring(92, 93);
			// 选区有效数据进行封装
			if (qua.matches("[01459]") && !tmp.equals("9999")) {
				YearStationTmp ys = new YearStationTmp(year, stationid, 0);
				double tmp_d = Double.parseDouble(tmp);
				DoubleWritable tmp_w = new DoubleWritable(tmp_d);
				// 输出数据
				context.write(ys, tmp_w);
			}
		}
	}

	public static class YSTCombiner extends Reducer<YearStationTmp, DoubleWritable, YearStationTmp, DoubleWritable> {
		@Override
		protected void reduce(YearStationTmp key, Iterable<DoubleWritable> values,
				Reducer<YearStationTmp, DoubleWritable, YearStationTmp, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			double maxTmp = 0;
			for (DoubleWritable value : values) {
				if (value.get() > maxTmp) {
					maxTmp = value.get();
				}
			}
			context.write(key, new DoubleWritable(maxTmp));
		}

	}

	public static class YSTReducer extends Reducer<YearStationTmp, DoubleWritable, YearStationTmp, NullWritable> {
		@Override
		protected void reduce(YearStationTmp key, Iterable<DoubleWritable> values,
				Reducer<YearStationTmp, DoubleWritable, YearStationTmp, NullWritable>.Context context)
				throws IOException, InterruptedException {
			double maxTmp = 0;
			for (DoubleWritable value : values) {
				if (value.get() > maxTmp) {
//					System.out.println("===" + key.toString() + "====" + maxTmp);
					maxTmp = value.get();
				}
			}
			YearStationTmp newKey = new YearStationTmp(key.getYear(), key.getStationId(), new DoubleWritable(maxTmp));
			context.write(newKey, NullWritable.get());
		}
	}
}
