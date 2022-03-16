package com.briup.hdfs.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ShowFileTest extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ShowFileTest(), args);
	}
	@Override
	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		Configuration conf=getConf();
		//conf.set("fs.defaultFS","hdfs://192.168.29.132:9000");
		System.out.println(conf);
		try { 
			FileSystem fs=FileSystem.get(conf);
			FSDataInputStream fsd=fs.open(new Path(conf.get("path")));
			//IOUtils.copyBytes(fsd,System.out,1024);
			String line=null;
			while((line=fsd.readLine())!=null) {
				System.out.println(line);
			}
			fsd.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
}

