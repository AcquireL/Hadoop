package com.briup.hdfs.common;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * 	展示hdfs文件系统上的文件存放在那个块
 *
 */

public class ShowBlocks extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ShowBlocks(), args);
	}
	public int run(String[] args) throws Exception {
		Configuration conf=getConf();
		FileSystem fs=FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path(conf.get("path")));
		HdfsDataInputStream hin=(HdfsDataInputStream)in;
		List<LocatedBlock> blocks = hin.getAllBlocks();
		for(LocatedBlock block:blocks) {
			System.out.println("-------------");
			//name id size location
			ExtendedBlock b = block.getBlock();
			System.out.println(b.getBlockId());
			System.out.println(b.getBlockName());
			System.out.println(block.getBlockSize());
			DatanodeInfo[] ls = block.getLocations();
			for(DatanodeInfo l:ls) {
				System.out.println(l.getHostName());
			}
		}
		return 0;
	}

}
