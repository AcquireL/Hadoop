package com.briup.hdfs.io;

import java.io.PrintWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

	//LocalFileSystem      会对校验和进行校验,同时生成 文件名.后缀.crc
	//RawLocalFileSystem 	不会
public class IntegrityWriteTest extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new IntegrityWriteTest(), args);
	}
		public int run(String[] args) throws Exception {
			Configuration conf=getConf();
			Path rPath=new Path(conf.get("rpath"));
			Path lPath=new Path(conf.get("lpath"));
			RawLocalFileSystem rlfs = new RawLocalFileSystem();
			rlfs.initialize(URI.create(conf.get("rpath")), conf);
			FSDataOutputStream rout = rlfs.create(rPath);
			PrintWriter rpw=new PrintWriter(rout);
			rpw.println("hhhhhhhhhhhh");
			rpw.flush();
			rpw.close();
			rlfs.close();
			//------------------------
			LocalFileSystem lfs = FileSystem.getLocal(conf);
			FSDataOutputStream lout = lfs.create(lPath);
			PrintWriter lpw=new PrintWriter(lout);
			lpw.println("hhhhhhhhhhhh");
			lpw.flush();
			lpw.close();
			return 0;
		}
}
