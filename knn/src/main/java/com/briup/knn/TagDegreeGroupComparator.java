package com.briup.knn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 *    分组比较器，用于GetTop类中
 *    	按照TagDegree实体类中的专门用于排序的group字段，进行升序排列
 */
public class TagDegreeGroupComparator extends WritableComparator{

	public TagDegreeGroupComparator() {
		super(TagDegree.class,true);
	}

	@Override
	public int compare(WritableComparable a,WritableComparable b) {
		TagDegree t1=(TagDegree)a;
		TagDegree t2=(TagDegree)b;
		return t1.getGroup().compareTo(t2.getGroup());
	}

}
