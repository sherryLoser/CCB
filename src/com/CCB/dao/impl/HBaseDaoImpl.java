package com.CCB.dao.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.CCB.bean.LogBean;
import com.CCB.dao.AppLog;
import com.CCB.dao.HbaseInter;
import com.CCB.dao.LogDao;
import com.CCB.util.FileUtils;
import com.CCB.util.HBaseUtils;
import com.CCB.util.HdfsUtil;
import com.CCB.util.SimpleDateUtil;

public class HBaseDaoImpl implements HbaseInter,AppLog {

	Configuration config = HBaseUtils.conf;

	/**
	 * function:上传文件到HBase
	 * 
	 * @param rowKey
	 *            主键：具有唯一性
	 * @param fileLocalPath
	 *            文件路径 ：要上传的文件路径
	 */
	public boolean UpToHBase(String rowKey, String fileLocalPath, String tableName, String columnFamily,
			String columns) {
		HTable table = null;
		HBaseAdmin hBaseAdmin = null;
		boolean flag = false;
		try {
			hBaseAdmin = new HBaseAdmin(config);
			flag = hBaseAdmin.tableExists(tableName);
			if (!flag) {
				HTableDescriptor htd = new HTableDescriptor(tableName);
				HColumnDescriptor col = new HColumnDescriptor(columnFamily);
				htd.addFamily(col);
				hBaseAdmin.createTable(htd);
			}
			table = new HTable(config, tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			byte[] b = FileUtils.toByteArray3(fileLocalPath);
			put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(columns), b);
			table.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOG.error(e.toString());
			System.out.println("文件不存在，或者上传失败!");
			return false;
		} finally {
			HBaseUtils.close(table);
			HBaseUtils.close(hBaseAdmin);
		}
		return true;
	}

	/**
	 * function:从HBase下载文件到本地路径
	 * 
	 * @param rowKey
	 *            主键：具有唯一性,根据rowkey获取文件
	 * @param fileLocalPath
	 *            文件路径 ：下载文件到该路径下
	 */
	public boolean DownFromHBase(String rowKey, String fileLocalPath, String tableName, String columnFamily,
			String columns) {
		HTable table = null;
		try {
			File file = new File(fileLocalPath);
			if (!file.getParentFile().exists()) {
				file.getParentFile().mkdirs();
			} else {
				file.delete();
			}
			table = new HTable(config, tableName);
			Get get = new Get(Bytes.toBytes(rowKey));
			Result result = table.get(get);
			FileOutputStream fos = new FileOutputStream(file);
			byte[] b = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(columns));
			System.out.println(new String(b));
			int len = b.length;
			int index = 0;
			while (index < len) {
				if (index + 1024 < len) {
					fos.write(b, index, 1024);
					index += 1024;
				} else {
					fos.write(b, index, len - index);
					break;
				}
			}
			fos.close();
			System.out.println("下载成功");
		} catch (IOException e1) {
			e1.printStackTrace();
			LOG.error(e1.toString());
			System.out.println("下载失败");
			return false;
		} finally {
			HBaseUtils.close(table);
		}
		return true;
	}

	/**
	 * function:根据rowkey删除文件
	 * 
	 * @param rowKey
	 *            主键:具有唯一性,根据rowkey删除文件
	 */
	public void DeleteFileByRowKey(String rowKey, String tableName) {
		HBaseUtils.deleteAllColumn(tableName, rowKey);
		System.out.println("删除成功");
	}

	/**
	 * function:根据路径将hdfs上的文件单个上传到hbase中
	 * 
	 * @param rowKey
	 *            主键:具有唯一性
	 *
	 */
	public boolean UpToHbase(String rowKey, String columnFamily, String columns, String tableName, String hdfsPath) {
		boolean flag = false;
		HTable table = null;
//		HBaseAdmin hBaseAdmin = null;
		try {
//			hBaseAdmin = new HBaseAdmin(config);
//			flag = hBaseAdmin.tableExists(tableName);
//			if (!flag) {
//				HTableDescriptor htd = new HTableDescriptor(tableName);
//				HColumnDescriptor col = new HColumnDescriptor(columnFamily);
//				htd.addFamily(col);
//				hBaseAdmin.createTable(htd);
//			}
			table = new HTable(config, tableName);
			byte[] content = HdfsUtil.readFromHdfs(hdfsPath);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(columns), content);
			table.put(put);
			flag = true;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			LOG.error(e.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOG.error(e.toString());
		} finally {
			HBaseUtils.close(table);
			//HBaseUtils.close(hBaseAdmin);
		}
		return flag;
	}

	/**
	 * function:根据文件夹路径将hdfs上的文件夹下的文件全部上传到hbase中
	 * 
	 * @param rowKey
	 *            主键:具有唯一性
	 *
	 */
	public boolean UpToHbases(String tableName, String columnFamily, String columns, Map<String, String> pathName) {
		boolean flag = false;
		HTable table = null;
		try {
//			hBaseAdmin = new HBaseAdmin(config);
//			flag = hBaseAdmin.tableExists(tableName);
//			if (!flag) {
//				HTableDescriptor htd = new HTableDescriptor(tableName);
//				HColumnDescriptor col = new HColumnDescriptor(columnFamily);
//				htd.addFamily(col);
//				hBaseAdmin.createTable(htd);
//			}
			table = new HTable(config, tableName);
			// List<String> nameList = HdfsUtil.listFile(hdfsPath);
			System.out.println(pathName.size());
			String path = "";
			String rowKey = "";
			List<Put> puts = new ArrayList<>(110);
			List<LogBean> logBeans = new ArrayList<>(110);
			LogDao logDao = new LogDao();
			int i = 0;
//			long ss = 0;
			for (Map.Entry<String, String> entry : pathName.entrySet()) {
				path = entry.getKey();
				if (!path.endsWith(".tmp")) {
					i++;
					rowKey = entry.getValue();
					byte[] content = HdfsUtil.readFromHdfs(path);
//					ss+=content.length;
					//System.out.println(ss+=Bytes.toBytes(rowKey).length);
					logBeans.add(logDao.getLogBean(rowKey, new String(content)));
					Put put = new Put(Bytes.toBytes(rowKey));
					put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(columns), content);
					table.put(put);
					if(i % 100 == 0 && i !=0){
						table.put(puts);
						logDao.creatIndex(logBeans);
						logBeans = new ArrayList<>(110);
						puts = new ArrayList<>(110);
					}
				}
			}
			table.put(puts);
			logDao.creatIndex(logBeans);
			flag = true;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			LOG.error(e.toString());
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error(e.toString());
		} finally {
			HBaseUtils.close(table);
			//HBaseUtils.close(hBaseAdmin);
		}
		return flag;
	}

	/**
	 * function:从HBase读取文件为String
	 * 
	 * @param rowKey
	 *            主键：具有唯一性,根据rowkey获取文件
	 * @param fileLocalPath
	 *            文件路径 ：下载文件到该路径下
	 */
	public String DownFromHBaseToString(String rowKey, String tableName, String columnFamily, String columns) {
		String content = null;
		HTable table = null;
		try {
			table = new HTable(config, tableName);
			Get get = new Get(Bytes.toBytes(rowKey));
			Result result = table.get(get);
			byte[] b = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(columns));
			content = new String(b);
			System.out.println("下载成功");
		} catch (IOException e1) {
			e1.printStackTrace();
			LOG.error(e1.toString());
			System.out.println("下载失败");
		} finally {
			HBaseUtils.close(table);
		}
		return content;
	}

	public boolean UpToHbases(String tableName, String columnFamily, String columns, String hdfsPath) {
		boolean flag = false;
		HTable table = null;
//		HBaseAdmin hBaseAdmin = null;
		try {
//			hBaseAdmin = new HBaseAdmin(config);
//			flag = hBaseAdmin.tableExists(tableName);
//			if (!flag) {
//				HTableDescriptor htd = new HTableDescriptor(tableName);
//				HColumnDescriptor col = new HColumnDescriptor(columnFamily);
//				htd.addFamily(col);
//				hBaseAdmin.createTable(htd);
//			}
			table = new HTable(config, tableName);
			List<String> nameList = HdfsUtil.listFile(hdfsPath);
			String fileName = "";
			String rowKey = "";
			for (String path : nameList) {
				byte[] content = HdfsUtil.readFromHdfs(path);
				fileName = path.substring(path.lastIndexOf("/") + 1, path.length());
				rowKey = fileName.substring(0, fileName.lastIndexOf(".")) + "+" + SimpleDateUtil.convert2String(
						Long.parseLong(fileName.substring(fileName.lastIndexOf(".") + 1)), SimpleDateUtil.TIME_FORMAT);
				Put put = new Put(Bytes.toBytes(rowKey));
				put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(columns), content);
				table.put(put);
				HdfsUtil.rmFile(path);
			}
			flag = true;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			LOG.error(e.toString());
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error(e.toString());
		} finally {
			HBaseUtils.close(table);
//			HBaseUtils.close(hBaseAdmin);
		}
		return flag;
	}

	public boolean UpToHbasesBatch(String tableName, String columnFamily, String columns, String hdfsPath) {
		boolean flag = false;
		HTable table = null;
		HBaseAdmin hBaseAdmin = null;
		try {
//			hBaseAdmin = new HBaseAdmin(config);
//			flag = hBaseAdmin.tableExists(tableName);
//			if (!flag) {
//				HTableDescriptor htd = new HTableDescriptor(tableName);
//				HColumnDescriptor col = new HColumnDescriptor(columnFamily);
//				htd.addFamily(col);
//				hBaseAdmin.createTable(htd);
//			}
			// HNAPTSYJ+OS+HPUX+11.31+0+syslog.log+2016-04-01 16:59:34
			table = new HTable(config, tableName);
			List<String> nameList = HdfsUtil.listFile(hdfsPath);
			String fileName = "";
			String rowKey = "";
			List<Put> puts = new ArrayList<>();
			for (String path : nameList) {
				if (!path.endsWith(".tmp")) {
					byte[] content = HdfsUtil.readFromHdfs(path);
					fileName = path.substring(path.lastIndexOf("/") + 1, path.length());
					rowKey = fileName.substring(0, fileName.lastIndexOf(".")) + "+"
							+ SimpleDateUtil.convert2String(
									Long.parseLong(fileName.substring(fileName.lastIndexOf(".") + 1)),
									SimpleDateUtil.TIME_FORMAT);
					Put put = new Put(Bytes.toBytes(rowKey));
					put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(columns), content);
					puts.add(put);
					HdfsUtil.rmFile(path);
				}
			}
			table.put(puts);
			flag = true;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			LOG.error(e.toString());
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error(e.toString());
		} finally {
			HBaseUtils.close(table);
			//HBaseUtils.close(hBaseAdmin);
		}
		return flag;
	}

	/**
	 * 
	 */
	public boolean insertBatch(Map<String, String> keyValues, String tableName, String columnFamily, String columns) {
		//boolean flag = createTable(tableName, columnFamily);
		boolean flag = true;
		HTable table = null;
		if (flag) {
			try {
				table = new HTable(config, tableName);
				List<Put> puts = new ArrayList<Put>();
				for (Map.Entry<String, String> entry : keyValues.entrySet()) {
					Put put = new Put(Bytes.toBytes(entry.getKey()));
					put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(columns), Bytes.toBytes(entry.getValue()));
					puts.add(put);
				}
				table.put(puts);
				flag = true;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				flag = false;
				e.printStackTrace();
				LOG.error(e.toString());
			}
		}
		return flag;
	}

	public boolean createTable(String tableName, String columnFamily) {
		boolean flag = false;
		HBaseAdmin hBaseAdmin = null;
		try {
			hBaseAdmin = new HBaseAdmin(config);
			flag = hBaseAdmin.tableExists(tableName);
			if (!flag) {
				HTableDescriptor htd = new HTableDescriptor(tableName);
				HColumnDescriptor col = new HColumnDescriptor(columnFamily);
				htd.addFamily(col);
				hBaseAdmin.createTable(htd);
			}
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error(e.toString());
		} finally {
			HBaseUtils.close(hBaseAdmin);
		}
		return flag;
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		HBaseDaoImpl hBaseProcess = new HBaseDaoImpl();
//		boolean flag = hBaseProcess.createTable("song", "Info");
		// System.out.println(hBaseProcess.DownFromHBaseToString("HNAPTSYJ+OS+HPUX+11.31+0+syslog.log+2016-04-01
		// 13:59:36", "testsong",
		// "cf1", "content"));
		// List<HBaseDao> daos = HBaseUtils.getResultScan("testsong");
		// System.out.println(daos.toString());
		//hBaseProcess.UpToHbasesBatch("testsong", "cf1", "content", "/Flume");
		// System.out.println(hBaseProcess.UpToHbase("111", "cf1", "content",
		// "testsong", "hdfs://hdp-1:8020/tmp/test2.txt"));
		// hBaseProcess.UpToHBase("123", "C:/Users/Sherry/Desktop/test (2).txt",
		// "songtest", "cf1", "content");
		// hBaseProcess.DownFromHBase("hs_err_pid14567.log",
		// "C:/Users/Sherry/Desktop/test(3).txt", "testsong", "cf1", "content");
		System.out.println(hBaseProcess.DownFromHBaseToString("100126+20160609010001+f5bc771ae0da1ae6a1052ff4cee20ea2", "config", "info", "value"));
	}
}
