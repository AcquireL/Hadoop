package com.briup.hdfs.io;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class IntegrityReadTest extends Configured  implements Tool{
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new IntegrityReadTest(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf=getConf();
		FileSystem.get(conf);
		@SuppressWarnings("resource")
		RawLocalFileSystem  rlf=new RawLocalFileSystem();
		rlf.initialize(URI.create(conf.get("rpath")),conf);
		FSDataInputStream rin=rlf.open(new Path(conf.get("rpath")));
		BufferedReader rr=new BufferedReader(new InputStreamReader(rin));
		String rline=rr.readLine();
		System.out.println(rline);
		rr.close();
		//---------------
		LocalFileSystem lfs=FileSystem.getLocal(conf);
		FSDataInputStream lin=lfs.open(new Path(conf.get("lpath")));
		BufferedReader lr=new BufferedReader(new InputStreamReader(lin));
		String lline=lr.readLine();
		System.out.println(lline);
		lr.close();
		return 0;
	}

}
