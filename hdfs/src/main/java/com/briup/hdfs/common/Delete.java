package com.briup.hdfs.common;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 *   ɾ��hdfs�ļ�ϵͳ�ϵ��ļ�
 */
public class Delete extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Delete(), args);
	}
	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		Configuration conf=getConf();
		FileSystem fs=FileSystem.get(conf);
		fs.delete(new Path(conf.get("path")));
		return 0;
	}
	
}
