package com.briup.hdfs.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/*
 * 	使用最基本的方法读取hdfs文件系统上的文件内容
 */

public class ShowFileContent {
	public static void main(String[] args) {
		//1 获取配置对象
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS","hdfs://192.168.29.132:9000");
		System.out.println(conf);
		try {
			//2 获取到文件系统对象
			FileSystem fs=FileSystem.get(conf);
		    //3 选取合适的流完成需求
			System.out.println(fs.getClass());
			FSDataInputStream in = fs.open(new Path("/user/hdfs/world.txt"));
			//FSDataInputStream in=fs.open(new  Path(args[0]));
			IOUtils.copyBytes(in,System.out,1024);
		/*	String line = null;
			while((line=in.readLine())!=null) {
				System.out.println();
			}*/
			//关闭文件流
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
