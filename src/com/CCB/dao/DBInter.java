package com.CCB.dao;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.CCB.bean.QueueTmp;

public interface DBInter {
	public List<String> select(String sql,String key);
	public boolean addstategyInfoToLocalMySQL(List<QueueTmp> queueTmps);
	public List<String> selectPool(String sql, String[] key);
	public boolean addHbaseTmpLocalPool(List<QueueTmp> queueTmps);
	public boolean addHbaseQueueLocalPool(List<QueueTmp> queueTmps);
	public boolean addLocalNewDataPool(Map<String, String> newDatas);
	public boolean addLocalWindowPageOutPool(Map<String, Double> newDatas);
	//public boolean addstategyInfoToLocalMySQL(QueueTmp queueTmp);
	public boolean addLocalsItemPool(HashSet<String> keys);
	public String addPool(String devname);
}
