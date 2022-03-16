package com.briup.hdfs.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ShowTree extends Configured implements Tool{
    FileSystem fs;
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new ShowTree(), args);

    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=getConf();
		/*Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.29.132:9000");*/
        System.out.println(conf);
        fs = FileSystem.get(conf);
        //拿到某个路径下的所有目录和子文件的元数据
        FileStatus[] list = fs.listStatus(new Path(conf.get("path")));
        //FileStatus[] list=fs.listStatus(new Path("/"));
        //System.out.println(list[4]);
        //show(list);
        for(FileStatus fs:list) {
            show(fs);
        }
        return 0;
    }
    public void show(FileStatus sta) {
        if(sta.isFile()&&sta.getLen()>0) {
            showDetail(sta);
        }else if(sta.isDirectory()){
            //获得到该目录下的一级文件的元数据
            //继续对每个元数据调用show
		/*	try {
				Stream.of(fs.listStatus(sta.getPath())).forEach(this::show));
			} catch (Exception e) {
				e.printStackTrace();
			}*/
            showDetail(sta);
            System.out.println("-------------------");
            try {
                FileStatus[] substas = fs.listStatus(sta.getPath());
                for(FileStatus substa:substas) {
                    show(substa);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    private void showDetail(FileStatus sta) {
        System.out.println(sta.getPath()+"   "+sta.getLen()+"  "
                +sta.getOwner()+"  "+sta.getAccessTime());
    }

}
