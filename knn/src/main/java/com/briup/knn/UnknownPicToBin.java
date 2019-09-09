package com.briup.knn;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/*
 * Knn第二步：
 * 	将待识别的图片二值化，并存入hdfs文件系统
 * 		1.输入为：Ubuntu本机   
 * 		2.输出位置：hdfs文件系统 路径：/knn_data/unknown
 */
public class UnknownPicToBin extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new UnknownPicToBin(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration conf=getConf();
		//Path inpath =new Path(conf.get("inpath"));
		Path inpath =new Path("/home/hdfs/train/0_13.png");
		Path outpath=new Path("/knn_data/unknown");
		putToHdfs(inpath,outpath,conf);
		return 0;
	}

	public static void putToHdfs(Path inpath, Path outpath, Configuration conf) throws Exception {
		// 本地文件系统
		LocalFileSystem local = FileSystem.getLocal(conf);
		// hdfs文件系统
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = local.open(inpath);
		FSDataOutputStream out = fs.create(outpath);
		// 二值化过程
		picToBin(in,out,true);
	}

	public static void picToBin(InputStream in, OutputStream out, boolean close) throws Exception {
		// BufferedImage
		BufferedImage img = ImageIO.read(in);
		//out沒有輸出字符的方法
		//所以把out包裝成字符流
		PrintWriter writer=new PrintWriter(out);
		int height = img.getHeight();
		int width = img.getWidth();
		for (int i = 0; i < height; i++) {
			for (int j = 0; j < width; j++) {
				int rgb = img.getRGB(j, i);
				Color gray = new Color(127, 127, 127);
				int g_rgb = gray.getRGB();
				if (rgb < g_rgb) {
					System.out.print("0");
					writer.write("0");
					writer.flush();
				} else {
					System.out.print("1");
					writer.write("1");
					writer.flush();
				}
			}
			System.out.println();
		}
		if(close) {
			in.close();
			writer.close();
		}
	}
}
