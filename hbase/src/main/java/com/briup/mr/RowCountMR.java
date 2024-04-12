package com.briup.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class RowCountMR extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run (new RowCountMR (), args);
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=getConf ();
        conf.set("hbase.zookeeper.quorum","master:2181");
        Job job=Job.getInstance (conf, "lwj_rowcount");
        job.setJarByClass (this.getClass ());
        TableMapReduceUtil.initTableMapperJob (conf.get ("intable"), new Scan (), RCMapper.class,Text.class, IntWritable.class,job);
        TableMapReduceUtil.initTableReducerJob (conf.get ("outtable"), RCReducer.class, job);
        job.waitForCompletion (true);
        return 0;
    }
    public  static class RCMapper extends TableMapper<Text, IntWritable> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            TableSplit s = (TableSplit) context.getInputSplit ();
            String name = new String (s.getTableName ());
            //表名,1
            context.write (new Text (name), new IntWritable (1));
        }
    }
    public static class RCReducer extends TableReducer<Text,IntWritable,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Stream<IntWritable> stream = StreamSupport.stream (values.spliterator (), false);
            long sum=stream.count();
            Put put=new Put(key.getBytes ());
            put.addColumn (Bytes.toBytes ("info"), Bytes.toBytes ("number"), Bytes.toBytes (sum+""));
            context.write (NullWritable.get (), put);
        }
    }

}
