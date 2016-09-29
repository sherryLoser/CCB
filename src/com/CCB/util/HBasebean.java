package com.CCB.util;

import java.util.HashMap;
import java.util.Map;

/**
 * @Date	2015-11-26
 * @function	HBaseDao对象对应HBase中的一行记录	
 * @author ljj
 *1:boolean exists(Object key)	判断family是否存在
 *2:HashMap<String, String> getMapByKey(Object key)	根据family获取family下的columns和values
 */
public class HBasebean {

	//rowkey
	private String rowKey;
	//保存一行记录
	private Map<String, HashMap<String, byte[]>> map = new HashMap<String, HashMap<String, byte[]>>(); 

	public String getRowKey() {
		return rowKey;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	public Map<String, HashMap<String, byte[]>> getMap() {
		return map;
	}

	public void setMap(Map<String, HashMap<String, byte[]>> map) {
		this.map = map;
	}

	/**
	 * 判断family是否存在
	 * @param key
	 * @return
	 */
	public boolean exists(Object key){
		if(map.containsKey(key))
			return true;
		return false;
	}
	
	/**
	 * 根据family获取family下的columns和values
	 * @param key
	 * @return
	 */
	public HashMap<String, byte[]> getMapByKey(Object key){
		if(exists(key)){
//			System.out.println(map.get(key));
			return map.get(key);
		}
		return null;
	}

	@Override
	public String toString() {
		return "HBaseDao [rowKey=" + rowKey + ", map=" + map + "]";
	}
	
	
	
	

}
