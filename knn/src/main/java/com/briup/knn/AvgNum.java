package com.briup.knn;

/*
 * 	用于GetTopK中
 * 		num：和待识别图片相同的个数
 * 		avg：和待识别图片相同的个数的平均相似度
 */
public class AvgNum {
	private int num;
	private double avg;
	public AvgNum() {
	}
	public AvgNum(int num, double avg) {
		this.num = num;
		this.avg = avg;
	}
	public int getNum() {
		return num;
	}
	public void setNum(int num) {
		this.num = num;
	}
	public double getAvg() {
		return avg;
	}
	public void setAvg(double avg) {
		this.avg = avg;
	}
	
}
