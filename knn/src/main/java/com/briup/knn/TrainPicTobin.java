package com.briup.knn;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/*
 * knn第一步：
 *       将图片库中的所有图片训练成数据集，数据集以向量的形式存在。
 *      1.文件的输入位置 hdfs文件系统：路径：/knn_data/train
 *      2.文件的输出位置 hdfs文件系统：路径：/knn_data/train_bin 文件内容存储格式：SequenceFile
 *   	注意：SequenceFile文件格式的构建
 *   
 */
public class TrainPicTobin extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new TrainPicTobin(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf=getConf();
		Path inpath=new Path("/knn_data/train");
		Path outpath=new Path("/knn_data/train_bin");
		allPicsTobin(inpath, outpath, conf);
		return 0;
	}

	public static void allPicsTobin(Path inpath, Path outpath, Configuration conf) throws Exception {
		// inpath,hdfs中的目录
		FileSystem fs = FileSystem.get(conf);
		// 通过文件系统获取某个目录下的所有文件
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(inpath, true);
		// 读取每个文件内容，二值化，输出到seqfile
		// 选项1 seqfile输出路径
		SequenceFile.Writer.Option option1 = SequenceFile.Writer.file(outpath);
		// 选项2 seqfile key类型，文件前缀名
		SequenceFile.Writer.Option option2 = SequenceFile.Writer.keyClass(Text.class);
		// 选项3 seqfile value类型，二值化向量
		SequenceFile.Writer.Option option3 = SequenceFile.Writer.valueClass(Text.class);
		// seqfile 输出流
		SequenceFile.Writer writer = SequenceFile.createWriter(conf, option1, option2, option3);
		// 用来接收key和value的值
		Text k = new Text();
		Text v = new Text();
		while (files.hasNext()) {
			// 二值化每个图片，把二值化结果设置为v
			LocatedFileStatus file = files.next();
			// 把图片的前缀名设置成k
			String name = file.getPath().getName();
			String new_name = name.substring(0, name.indexOf("."));
			k.set(new_name);
			FSDataInputStream in = fs.open(file.getPath());
			// 调用二值化方法
			String bin_line = picTobin(in);
			v.set(bin_line);
			// 追加到seqfile
			writer.append(k, v);
		}
		writer.close();
	}

	private static String picTobin(FSDataInputStream in) throws Exception, IOException {
		// BufferedImage
		BufferedImage img = ImageIO.read(in);
		StringBuffer sb = new StringBuffer();
		int height = img.getHeight();
		int width = img.getWidth();
		for (int i = 0; i < height; i++) {
			for (int j = 0; j < width; j++) {
				int rgb = img.getRGB(j, i);
				Color gray = new Color(127, 127, 127);
				int g_rgb = gray.getRGB();
				if (rgb < g_rgb) {
					System.out.print("0");
					sb.append("0");
				} else {
					System.out.print("1");
					sb.append("1");
				}
			}
			System.out.println();
		}
		return sb.toString();
	}

}
