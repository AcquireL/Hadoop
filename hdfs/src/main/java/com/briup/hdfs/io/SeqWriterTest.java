package com.briup.hdfs.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SeqWriterTest
	extends Configured implements Tool {
	public static String[] vs =
			{"hello world",
			"test test",
			"briup briup",
			"bd1803 bd1803"};
	
	@Override
	public int run(String[] strings)
		throws Exception {
		Configuration conf = getConf();
		
		IntWritable key = new IntWritable();
		Text value = new Text();
		
		SequenceFile.Writer.Option op1 =
			SequenceFile.Writer.file
				(new Path(conf.get("path")));
		SequenceFile.Writer.Option op2 =
			SequenceFile.Writer.keyClass
				(key.getClass());
		SequenceFile.Writer.Option op3 =
			SequenceFile.Writer.valueClass
				(value.getClass());
		
/*		SequenceFile.Writer.Option op4 =
			SequenceFile.Writer.compression
				(SequenceFile.CompressionType.RECORD,
					new GzipCodec());*/
		

		//�����
		// �ļ�����key�����ͣ�value������
		SequenceFile.Writer writer =
			SequenceFile.createWriter
				(conf, op1, op2, op3);

		for (int i = 0; i < 100 ; i++) {
			key.set(i);
			value.set(vs[i%vs.length]);
			if(i%3 == 0){
				writer.sync();
			}
			writer.append(key,value);
		}

		writer.close();
		return 0;
	}

	public static void main(String[] args)
		throws Exception {
		ToolRunner.run
			(new SeqWriterTest(),args);
	}
}
