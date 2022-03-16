package com.briup.hdfs.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/*
 * 	把hdfs系统上的压缩文件 读取到本地（自动解压）
 */
public class CompressionReadTest extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new CompressionReadTest(), args);
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=getConf();
        FileSystem fs=FileSystem.get(conf);
        LocalFileSystem lfs=FileSystem.getLocal(conf);
        FSDataInputStream fis=fs.open(new Path(conf.get("inpath")));
        FSDataOutputStream fos=lfs.create(new Path(conf.get("outpath")));

        CompressionCodecFactory factory=new CompressionCodecFactory(conf);
        CompressionCodec codec=factory.getCodec(new Path(conf.get("inpath")));
        CompressionInputStream cis=codec.createInputStream(fis);
        IOUtils.copyBytes(cis, fos, 128,true);

        return 0;
    }



}
