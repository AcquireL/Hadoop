package com.briup.knn;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.FileInputStream;

import javax.imageio.ImageIO;
/*
 *	测试图片的二值化过程
 *	将一张图片进行二值化
 *		1.根据图片的像素点的位置，获取图片的RGB色值，与灰色的的RGB色值相比较
 *		2.小于灰色的值，将该像素点设置为0，大于设置为1
 */
public class BinTest {
	public static void main(String[] args) throws Exception {
		//BufferedImage
		BufferedImage img=ImageIO.read(new FileInputStream("src/main/java/unknown.png"));
		int height=img.getHeight();
		int width=img.getWidth();
		Color gray=new Color(127,127,127);
		int g_rgb=gray.getRGB();
		for(int i=0;i<height;i++) {
			for(int j=0;j<width;j++) {
				int rgb=img.getRGB(j,i);	
				if(rgb<g_rgb) {
					System.out.print("0");
				}else {
					System.out.print("1");
				}
			}
			System.out.println();
		}
	}
}
