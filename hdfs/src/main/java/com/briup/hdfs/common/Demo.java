package com.briup.hdfs.common;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Demo extends Configured  implements Tool{
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Demo(), args);
	}

	public int run(String[] args) throws Exception {
		
		return 0;
	}

}
