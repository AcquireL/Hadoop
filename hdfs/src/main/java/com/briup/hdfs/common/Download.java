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
 *  	从hdfs文件系统上下载文件到本地
 *
 */

public class Download extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Download(), args);
    }
    //下载
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=getConf();
        FileSystem fs=FileSystem.get(conf);
        //inpath 要从hdfs文件系统下载的文件的路径
        FSDataInputStream ds=fs.open(new Path(conf.get("inpath")));

        LocalFileSystem lfs=FileSystem.getLocal(conf);

        //outpath 放在本地的位置 注：要为文件取名
        FSDataOutputStream os=lfs.create(new Path(conf.get("outpath")));

        IOUtils.copyBytes(ds, os, 128, true);
        return 0;
    }


}
