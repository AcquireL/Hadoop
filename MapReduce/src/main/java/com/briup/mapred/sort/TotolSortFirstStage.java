package com.briup.mapred.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TotolSortFirstStage 
	extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new TotolSortFirstStage(), args);
	}
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Path in = new Path(conf.get("inpath"));
		Path out = new Path(conf.get("outpath"));
		
		Job job = Job.getInstance(conf,"ts_first");
		job.setJarByClass(TotolSortFirstStage.class);
		
		job.setMapperClass(PartialSort.PSMapper.class);
		job.setMapOutputKeyClass
			(DoubleWritable.class);
		job.setMapOutputValueClass
			(Text.class);
		
		job.setReducerClass
			(PartialSort.PSReducer.class);
		job.setOutputKeyClass
			(DoubleWritable.class);
		job.setOutputValueClass
			(Text.class);
		
		job.setInputFormatClass
			(TextInputFormat.class);
		job.setOutputFormatClass
			(SequenceFileOutputFormat.class);
		TextInputFormat.addInputPath(job, in);
		SequenceFileOutputFormat.
			setOutputPath(job, out);
		job.waitForCompletion(true);
		return 0;
	}
}
// ts_pre_result



