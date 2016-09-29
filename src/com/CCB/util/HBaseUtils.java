package com.CCB.util;


import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * @Date	2015-11-26
 * @function	HBase工具类
 * @author ljj
 *1: boolean existsTable(String tableName)	判断hbase表是否存在
 *2：boolean createTable(String tableName,String[] family)	创建表
 *3：void ShowTable(String tableName)	
 *4：void insertRecord(String tableName, String rowKey, String columnFamily,
			String[] columns, String[] values)
			插入数据
 *5:deleteRow(String tableName,List<String> rowKeyList)	根据rowkey集合删除记录
 *6：void dropTable(String tableName)	删除表
 *7：ArrayList<HBaseDao> getResultScan(String tableName)		返回hbase表中的所有数据		并转化成HBaseDao
 *8：insert(HBaseDao hd, Result r)	结果转化为HBaseDao
 *9：void modifyTable(String tableName, String[] columns) 	修改表  添加列
 *10：HBaseDao getResultScan(String tableName,String rowKey)		根据rowkey查询 并且转化为HBaseDao
 *11：ArrayList<HBaseDao> getResultScan(String tableName,List<String> rowkeyList)	根据rowkey集合查询		并且转化为HBaseDao集合
 *12：ArrayList<HBaseDao> getResultScan(String tableName,String startRowKey,String stopRowKey)	根据startRowKey和startRowKey进行查询	或者这个范围之内的数据	并转化为HBaseDao集合
 *13:HBaseDao getResultByColumn(String tableName,String rowKey,String family,String qualifier) 	获取某行某列簇下某列的值
 *14:void updateTable(String tableName,String rowKey,String family,String qualifier,String value)		修改某行某列簇下某列的值
 *15:HBaseDao getResultByVersion(String tableName,String rowKey,String family,String qualifier)		获取某行某列的多个版本
 *16:void deleteColumn(String tableName,String rowKey,String family,String qualifier)	删除列
 *17:void deleteAllColumn(String tableName,String rowKey)	删除某行所有列
 *18:void deleteTable(String tableName)	删除表
 */
public class HBaseUtils {
	public static Configuration conf;
	private static Properties hbase_config = null;
	public static String hbase_config_addr = null;
	static {
		System.setProperty("hadoop.home.dir", "C:/lib/hadoop");
		InputStream ins = HBaseUtils.class.getClassLoader()
				.getResourceAsStream("Hbase.properties");
		hbase_config = new Properties();
		try {
			hbase_config.load(ins);
			hbase_config_addr = hbase_config.getProperty("Hbase_Config_Addr");
			conf = HBaseConfiguration.create();
			conf.addResource(new Path(hbase_config_addr));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	/**
	 * 判断表是否存在
	 * 存在返回true
	 * 不存在返回false
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public static boolean existsTable(String tableName){

		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			if(admin.tableExists(tableName)){
				System.out.println("table Exists!");
				return true;
			}else{
				System.out.println("table not Exists!");
				return false;
			}
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 创建表
	 * @param tableName 
	 * 			表名
	 * @param columns
	 * 			列族
	 * @throws Exception
	 */
	public static boolean createTable(String tableName,String[] family){
		System.out.println("start create table!");
		HBaseAdmin admin;
		try {
			admin = new HBaseAdmin(conf);
			if(existsTable(tableName)){
				return false;
			}
			HTableDescriptor desc = new HTableDescriptor(tableName);
			
			for(int i=0;i<family.length;i++){
				desc.addFamily(new HColumnDescriptor(family[i]));
			}
			admin.createTable(desc);
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("create table Success!");
		return true;
	}
	
	/**
	 * 
	 * @param tableName
	 * @throws MasterNotRunningException
	 * @throws ZooKeeperConnectionException
	 * @throws IOException
	 */
	public static void ShowTable(String tableName){
		
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			System.out.println("step1");
			if(admin.tableExists(tableName)){
				HTableDescriptor desc = new HTableDescriptor(tableName);
				System.out.println(desc.getColumnFamilies().toString());
				System.out.println("show table seccess!");
			}
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * 插入一行记录
	 * 
	 * @param tablename
	 *            表名
	 * @param row
	 *            行名称
	 * @param columnFamily
	 *            列族名
	 * @param columns
	 *            （列族名：column）组合成列名
	 * @param values
	 *            行与列确定的值
	 */
	public static void insertRecord(String tableName, String rowKey, String columnFamily,
			String[] columns, String[] values) {
		HTable table = null;
		try {
			Configuration conf = HBaseConfiguration.create();
			table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			for (int i = 0; i < columns.length; i++) {
				put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(String
						.valueOf(columns[i])), Bytes.toBytes(values[i]));
				table.put(put);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			close(table);
		}
		System.out.println("insert seccess!");
	}
	
	
	public static void insertRecord(String tableName, String rowKey, String columnFamily,
			String columns, byte[] values) {
		HTable table = null;
		try {
			Configuration conf = HBaseConfiguration.create();
			table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(String.valueOf(columns)), values);
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			close(table);
		}
		System.out.println("insert seccess!");
	}
	
	
	/**
	 * 批量插入
	 * @param tableName
	 * @param rowKeys
	 * @param columnFamily
	 * @param columns
	 * @param values
	 */
	public static void insertRecords(String tableName, ArrayList<String> rowKeys, String columnFamily,
			String columns, ArrayList<byte[]> values) {
		HTable table = null;
		try {
			Configuration conf = HBaseConfiguration.create();
			table = new HTable(conf, tableName);
			List<Put> puts = new ArrayList<Put>(1010);
			for(int i=rowKeys.size()-1;i>=0;i--){
				Put put = new Put(Bytes.toBytes(rowKeys.get(i)));
				put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(String.valueOf(columns)), values.get(i));
				puts.add(put);
				if(i%1000==0&&i!=0){
					table.put(puts);
					puts = new ArrayList<Put>(1010);
				}
			}
			table.put(puts);
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			close(table);
		}
		System.out.println("insert seccess!");
	}
	
	
	/**
	 * 根据rowkey删除记录
	 * @param tableName
	 * @param rowKey
	 */
	public static void deleteRow(String tableName,List<String> rowKeyList){
		System.out.println("start delete rows by rowkeys!");
		HTable table = null;
		try {
			table = new HTable(conf, tableName);
			ArrayList<Delete> list = new ArrayList<Delete>();
			Delete d ;
			for(String rowKey:rowKeyList){
				d = new Delete(rowKey.getBytes());
				list.add(d);
			}
			table.delete(list);
			System.out.println("delete rows by rowkey seccess!");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			close(table);
		}
	}
	
	/**
	 * 删除表
	 * @param tableName
	 */
	public static void dropTable(String tableName){
		System.out.println("start delete table!");
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println(" delete table success!");
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 修改表信息
	 * 添加列
	 * @param tableName
	 * @param column
	 */
	public static void modifyTable(String tableName, String[] columns){
		HBaseAdmin admin;
		try {
			admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			for(int i=0;i<columns.length;i++){
				admin.modifyColumn(tableName, new HColumnDescriptor(columns[i]));
			}
			admin.enableTable(tableName);
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * 遍历hbase表
	 * @param tableName
	 */
	public static ArrayList<HBasebean> getResultScan(String tableName){
		HTablePool pool = new HTablePool();
		ResultScanner rs = null;
		ArrayList<HBasebean> hds = new ArrayList<HBasebean>();
		HBasebean hd;
		@SuppressWarnings("unused")
		String famliy="";
		try {
			rs = pool.getTable(tableName).getScanner(new Scan());
			for(Result r: rs){
				hd = new HBasebean();
				hd.setRowKey(new String(r.getRow(),StandardCharsets.UTF_8));
				insert(hd, r);
				hds.add(hd);
			}
			return hds;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if(rs!=null){
				rs.close();
			}
		}
		return null;
	}

	public static void insert(HBasebean hd, Result r) {
		String famliy;
		for(KeyValue kv:r.list()){
			famliy = new String(kv.getFamily(),StandardCharsets.UTF_8);
			if(!hd.exists(famliy)){
				hd.getMap().put(famliy,new HashMap<String,byte[]>());
			}
			hd.getMap().get(famliy).put(new String(kv.getQualifier(),StandardCharsets.UTF_8),kv.getValue());
//			System.out.println(new String(kv.getQualifier(),StandardCharsets.UTF_8)+"\t"+new String(kv.getValue(),StandardCharsets.UTF_8));
			hd.getMap().get(famliy).put("Timestamp", (kv.getTimestamp()+"").getBytes());
		}
	}
	
	/**
	 * 根据rowkey查询
	 * @param tableName
	 * @param rowKey
	 * @return
	 */
	public static HBasebean getResultScan(String tableName,String rowKey){
		HTable table = null;
		try {
			HBasebean hd = new HBasebean();
			hd.setRowKey(rowKey);
			Get get = new Get(rowKey.getBytes());
			table = new HTable(conf, tableName);
			Result result = table.get(get);
			@SuppressWarnings("unused")
			String famliy;
			insert(hd, result);
			return hd;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				if(null!=table){
					table.close();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;
		
	}
	
	
	public static byte[] getResultOfColumn(String tableName,String rowKey,String  columnFamily, String columns){
		HTable table;
		try {
			table = new HTable(conf, tableName);
			Get get = new Get(Bytes.toBytes(rowKey));
			Result result = table.get(get);
			if(result!=null){
				return result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(columns));
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	
/**
 * 获取一些rowkey的数据
 * @param tableName
 * @param rowkeyList
 * @return
 */
	public static ArrayList<HBasebean> getResultScan(String tableName,List<String> rowkeyList){
		
		HTable table = null;
		@SuppressWarnings("unused")
		String famliy;
		ArrayList<HBasebean> hds = new ArrayList<HBasebean>();
		HBasebean hd;
		try {
			table = new HTable(conf, tableName);
			Get get = null;
			List<Get> list = new ArrayList<Get>();
			for(String str:rowkeyList){
				get = new Get(Bytes.toBytes(str));
				list.add(get);
			}
			Result[] rs = table.get(list);
			for(Result r: rs){		
				hd = new HBasebean();
				hd.setRowKey(new String(r.getRow(),StandardCharsets.UTF_8));
				insert(hd, r);
				hds.add(hd);
			}
			return hds;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			close(table);
		}
		return null;
	
	}
	
	
	/**
	 * 获取某一个范围的数据
	 * @param tableName
	 * @param startRowKey
	 * @param stopRowKey
	 */
	public static ArrayList<HBasebean> getResultScan(String tableName,String startRowKey,String stopRowKey){
		Scan scan = new Scan();
		scan.setStartRow(startRowKey.getBytes());
		scan.setStopRow(stopRowKey.getBytes());
		ResultScanner rs = null;
		HTable table = null;
		@SuppressWarnings("unused")
		String famliy;
		ArrayList<HBasebean> hds = new ArrayList<HBasebean>();
		HBasebean hd;
		try {
			table = new HTable(conf, tableName);
			rs = table.getScanner(scan);
			for(Result r: rs){		
				hd = new HBasebean();
				hd.setRowKey(new String(r.getRow(),StandardCharsets.UTF_8));
				insert(hd, r);
				hds.add(hd);
			}
			return hds;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			close(table);
		}
		return null;
	
	}
	
	/**
	 * 获取表中某行的某列值
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 */
	public static HBasebean getResultByColumn(String tableName,String rowKey,String family,String qualifier){
		HTable table = null;
		try {
			HBasebean hd  = new HBasebean();
			hd.setRowKey(rowKey);
			table = new HTable(conf, tableName);
			Get get = new Get(rowKey.getBytes());
			//获取指定列族和列修饰符对应的列
			get.addColumn(family.getBytes(), qualifier.getBytes());
			Result r = table.get(get);
			insert(hd, r);
			return hd;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				if(null!=table){
					table.close();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;
	}
	
	/**
	 * 更新表的某一列
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param value
	 */
	public static void updateTable(String tableName,String rowKey,String family,String qualifier,String value){
		HTable table = null;
		try {
			table = new HTable(conf, tableName);
			Put put = new Put(rowKey.getBytes());
			put.add(family.getBytes(), qualifier.getBytes(), value.getBytes());
			table.put(put);
			System.out.println("update table Success!");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				if(null!=table){
					table.close();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	/**
	 * 查询某列数据的多个版本
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 */
	public static HBasebean getResultByVersion(String tableName,String rowKey,String family,String qualifier){
		HTable table = null;
		try {
			HBasebean hd = new HBasebean();
			hd.setRowKey(rowKey);
			table = new HTable(conf, tableName);
			Get get = new Get(rowKey.getBytes());
			get.addColumn(family.getBytes(), qualifier.getBytes());
			get.setMaxVersions(5);
			Result r = table.get(get);
			insert(hd, r);
			return hd;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			close(table);
		}
		return null;
	}
	
	public static void close(HTable table){
		try {
			if(null!=table){
				table.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void close(HBaseAdmin hBaseAdmin){
		try {
			if(null!=hBaseAdmin){
				hBaseAdmin.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * 删除指定的列
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 */
	public static void deleteColumn(String tableName,String rowKey,String family,String qualifier){
		HTable table = null;
		try {
			table = new HTable(conf, tableName);
			Delete deleteColumn = new Delete(rowKey.getBytes());
			deleteColumn.deleteColumn(family.getBytes(), qualifier.getBytes());
			table.delete(deleteColumn);
			System.out.println("family: "+family+"\t column: "+qualifier+" is deleted!");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			close(table);
		}
		
	}
	
	/**
	 * 删除所有的列
	 * @param tableName
	 * @param rowKey
	 */
	public static void deleteAllColumn(String tableName,String rowKey){
		HTable table = null;
		try {
			table = new HTable(conf, tableName);
			Delete deleteAll = new Delete(rowKey.getBytes());
			table.delete(deleteAll);
//			System.out.println("all columns are deleted");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			close(table);
		}
	}
	
	/**
	 * 删除表
	 * @param tableName
	 */
	public static void deleteTable(String tableName){
		HBaseAdmin admin;
		try {
			admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println(tableName+" is deleted!");
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void main(String[] args) {
		String[] familyNames = {"info"};
		createTable("configOther", familyNames);
	}
}
