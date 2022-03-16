package com.briup.hdfs.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/*
 * 	从本地上传文件到hdfs文件系统上
 */
public class Upload extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Upload(), args);
    }
    //上传
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=getConf();
        FileSystem fs = FileSystem.get(conf);
        //获得程序到hdfs的输出流   outpath:上传到集群上的位置
        FSDataOutputStream out=fs.create(new Path(conf.get("outpath")));

        LocalFileSystem localfs=FileSystem.getLocal(conf);
        //inpath:本地要上传的东西的路径
        FSDataInputStream in=localfs.open(new Path(conf.get("inpath")));
        IOUtils.copyBytes(in, out, 128, true);

        return 0;
    }

}
