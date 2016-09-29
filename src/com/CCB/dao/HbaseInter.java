package com.CCB.dao;

import java.util.Map;

public interface HbaseInter {
	public boolean UpToHBase(String rowKey, String fileLocalPath, String tableName,
			String columnFamily, String columns);
	public boolean DownFromHBase(String rowKey, String fileLocalPath, String tableName,
			String columnFamily, String columns);
	public void DeleteFileByRowKey(String rowKey,String tableName);
	public boolean UpToHbase(String rowKey, String columnFamily, String columns, String tableName,String hdfsPath);
	public boolean UpToHbases( String tableName, String columnFamily, String columns,Map<String,String> pathName);
	public String DownFromHBaseToString(String rowKey, String tableName,
			String columnFamily, String columns);
	public boolean UpToHbasesBatch(String tableName, String columnFamily, String columns, String hdfsPath);
	public boolean insertBatch(Map<String, String> keyValues, String tableName, String columnFamily,
			String columns);
}
