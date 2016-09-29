package com.CCB.main;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.CCB.bean.QueueTmp;
import com.CCB.bean.RsNet;
import com.CCB.dao.AppLog;
import com.CCB.dao.DBInter;
import com.CCB.dao.HbaseInter;
import com.CCB.dao.NetDao;
import com.CCB.dao.impl.DBDaoImpl;
import com.CCB.dao.impl.HBaseDaoImpl;
import com.CCB.dao.impl.NetDaoImpl;
import com.CCB.util.HdfsUtil;
import com.CCB.util.Md5Util;

public class Test implements AppLog{
	private static String netsmart = "networksmark.txt";
	private static String window_cpu_item = "CPUCpuUtil"; 
	private static String window_disk_item = "DSKPercentBusy";
	private static String window_MemPageOut_item = "MEMPageOut";
	private static float pageOut_Peri = 99;           //window系统MemoryUsage>=此数值时添加一条数据pageout=1
	private static float total_peri = 100;           // 系统磁盘总为100%
	
	private DBInter dbDao = new DBDaoImpl();
	private HbaseInter hBaseDao = new HBaseDaoImpl();
	String preDevNameGol = "";  //pre类变量的作用是防止访问数据库太频繁，记录上次devName，解析规则信息，下次如果查询条件相同就不访问数据库，直接使用上次记录的值
	String preDevIdGol = "";
	String preItemname = "";
	String preItemtype = "";
	String preLogName = "";
	String preRuleId = "";
	String preItemver = "";
	String preRule = "";

	String preDevIP = "";// 转储网络日志时使用
	String preProductor = "";// 转储网络日志时使用

	Map<String, String> pathName = new HashMap<String, String>();  //记录非结构化日志解析的文件的路径，以便删除是使用
	List<QueueTmp> queueTmps = new ArrayList<QueueTmp>(); //记录添加入mysql队列临时表的数据

	boolean fileTimeFlag = false; // 标志文件的时间是否在0点到0点十分之间

	public boolean fullFile(String hdfsPath) {
		boolean flag = true;
		List<String> nameList = null;
		try {
			nameList = HdfsUtil.listFile(hdfsPath);
			if (nameList.size() == 0)
				return flag;
			String fileName = "";
			String fullPath = "";
			String currentTime = String.valueOf(System.currentTimeMillis()); // 此次程序运行时间
			String fileTime = to14(currentTime.substring(fullPath.lastIndexOf(".") + 1));
			fileTimeFlag = timeRange(fileTime); // 时间是否在0点到0点10分之间
			for (int i = 0; i < nameList.size(); i++) {
				fullPath = nameList.get(i);
				fileName = fullPath.substring(fullPath.lastIndexOf("/") + 1);
				if (!fileName.contains(".tmp")) {
					//总行主机的性能日志、总行网络的性能日志、二级行的性能日志
					if (fileName.startsWith("HNDB4510") && fileName.contains("perfinfo.txt")
							|| fileName.contains(netsmart) || fileName.contains("branchsmart.txt")) {
						performance(fullPath);
					}else if (fileName.contains("networklog.txt")) //网络日志
						netLog(fileName, fullPath);
					else if (fileName.contains("configsmart.txt"))  //配置信息
						performancesss(fullPath);
//					else if (fileName.contains("history")){}   //spark Streaming使用的日志文件，不处理
					else
						log(fileName, fullPath);  //应用日志
				}
			}
			//非结构日志进行查询信息后存入hbase和mysql队列表
			if (pathName.size() != 0) {
				if (hBaseDao.UpToHbases("testsong", "cf1", "content", pathName)) {
					if (dbDao.addHbaseTmpLocalPool(queueTmps) && dbDao.addHbaseQueueLocalPool(queueTmps)) {
						for (Map.Entry<String, String> entry : pathName.entrySet())
							try {
								HdfsUtil.rmFile(entry.getKey());
							} catch (FileNotFoundException e) {
								e.printStackTrace();
								LOG.error(e.toString());
							} catch (IOException e) {
								e.printStackTrace();
								LOG.error(e.toString());
							}
					} else {
						System.out.println("插入mysql失败");
						LOG.info("插入mysql失败");
						flag = false;
					}
				} else {
					System.out.println("插入hbase记录失败");
					LOG.info("插入hbase记录失败");
					flag = false;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error(e.toString());
			return false;
		}
		return flag;
	}
	
	//应用日志根据文件名查询devid和解析规则
	public boolean log(String fileName, String fullPath) {
		boolean flag = true;
		String[] name = null;
		String time = "";
		String rowKey = "";
		String logname = "";
		String devId = "";
		String ruleId = "";
		String rule = "";
		StringBuffer sql = new StringBuffer();
		String[] keyIds = { "devid" };
		String[] keyRule = { "resolverule","resolveruleid" };
		name = fileName.split("\\+");
		if (name.length == 6 && !name[5].endsWith(".tmp")) {
			QueueTmp queueTmp = new QueueTmp();
			if (!name[0].equals(preDevNameGol)) {
				sql.append("select sql_cache devid from info_dev where devname=\"" + name[0] + "\" limit 1");
				List<String> devIds = dbDao.selectPool(sql.toString(), keyIds);
				sql.delete(0, sql.length());
				if (devIds != null && devIds.size() != 0) {
					devId = devIds.get(0);
				} else {
					// devId = dbDao.addPool(name[0]);
					System.out.println(fullPath + ":" + name[0] + "在数据表info_dev中查不到相应的设备记录");
					LOG.info(fullPath + ":" + name[0] + "在数据表info_dev中查不到相应的设备记录");
					return false;
				}
				preDevIdGol = devId;
				preDevNameGol = name[0];
			} else if("".equals(name[0])) {
				LOG.info("设备名为空");
				return false;
			}
			else 
				devId = preDevIdGol;
			logname = name[5].substring(0, name[5].lastIndexOf("."));
			if (!name[2].equals(preItemname) || !name[3].equals(preItemver) || !name[1].equals(preItemtype)
					|| !logname.equals(preLogName)) {
				sql.append(
						"select sql_cache rule.resolverule,rule.resolveruleid from logresolverule rule,param_itemtype type,param_itemname iname where "
								+ "type.itemtype=\"" + name[1] + "\" and iname.itemname=\"" + name[2]
								+ "\" and iname.itemver=\"" + name[3] + "\" and rule.logname=\"" + logname
								+ "\" and rule.itemnameid=iname.itemnameid and rule.itemtypeid=type.itemtypeid "
								+ " limit 1");
				List<String> ruleIds = dbDao.selectPool(sql.toString(), keyRule);
				sql.delete(0, sql.length());
				if (ruleIds != null && ruleIds.size() > 1) {
					rule = ruleIds.get(0);
					ruleId = ruleIds.get(1);
				} else {
					System.out.println(fullPath + ":" + fileName + "在数据表logresolverule查不到相应的解析规则");
					LOG.info(fullPath + ":" + fileName + "在数据表logresolverule查不到相应的解析规则");
					return false;
				}
				preRule = rule;
				preRuleId = ruleId;
				preItemtype = name[1];
				preItemname = name[2];
				preItemver = name[3];
				preLogName = logname;
			} else {
				rule = preRule;
				ruleId = preRuleId;
			}
			queueTmp.setResolverule(rule);
			queueTmp.setResolveruleid(ruleId);
			time = to14(name[5].substring(name[5].lastIndexOf(".") + 1));
			rowKey = fileName.substring(0, fileName.lastIndexOf(".")) + "+" + time;
			queueTmp.setDevid(devId);
			queueTmp.setLog_id(rowKey);
			queueTmp.setDevname(name[0]);
			queueTmp.setItemtype(name[1]);
			queueTmp.setItemname(name[2]);
			queueTmp.setItemvar(name[3]);
			queueTmp.setInstance(name[4]);
			queueTmp.setLogname(logname);
			queueTmp.setCollect_time(time); // 解析
			queueTmps.add(queueTmp);
			pathName.put(fullPath, rowKey);
		} else {
			System.out.println(fullPath + "的文件名不符合规范");
			LOG.info(fullPath + "的文件名不符合规范");
		}
		return flag;
	}

	//网络日志根据文件名查询devid和解析规则，解析规则的查询与普通日志的查询有差别
	public boolean netLog(String fileName, String fullPath) {
		boolean flag = true;
		String[] name = null;
		String time = "";
		StringBuffer rowKey = new StringBuffer();
		String logname = "";
		String devId = "";
		String ruleId = "";
		String rule = "";
		String productor = "";
		String devName = "";
		String[] keyDev = { "devid", "devname", "productor" };
		String[] keyRule = { "resolverule" };
		StringBuffer sql = new StringBuffer();
		name = fileName.split("\\+");
		if (name[0].contains("re0")) {
			name[0] = name[0].replace("_re0", "");
		}
		if (name.length == 6 && !name[5].endsWith(".tmp")) {
			QueueTmp queueTmp = new QueueTmp();
			if (!name[0].equals(preDevIP)) {
				//传过来的文件名中包含的可能为主机名或者IP，根据主机名或者IP查询productor产品名称
				sql.append("select sql_cache devid,devname,productor from info_dev,param_productor prod where (devip=\""
						+ name[0] + "\" or devname=\"" + name[0]
						+ "\") and prod.productorid=info_dev.productorid limit 1");
				List<String> devIds = dbDao.selectPool(sql.toString(), keyDev);
				sql.delete(0, sql.length());
				if (devIds != null && devIds.size() != 0) {
					devId = devIds.get(0);
					devName = devIds.get(1); 
					productor = devIds.get(2);
				} else {
					// devId = dbDao.addPool(name[0]);
					if (devIds == null){
						System.out.println("连接数据库失败！！！");
						LOG.info("连接数据库失败！！！");
					}
					else{
						System.out.println(fullPath + ":" + name[0] + "在数据表info_dev中查不到相应的设备记录");
						LOG.info(fullPath + ":" + name[0] + "在数据表info_dev中查不到相应的设备记录");
					}
					return false;
				}
				preDevIdGol = devId;
				preDevNameGol = devName;
				preDevIP = name[0];
			} else {
				devId = preDevIdGol;
				productor = preProductor;
				devName = preDevNameGol;
			}
			logname = name[5].substring(0, name[5].lastIndexOf("."));
			
			//根据productor，itemtype，itemname，logname查询解析规则
			if (!name[1].equals(preItemtype) || !productor.equals(preProductor) || !logname.equals(preLogName)) {
				sql.append(
						"select sql_cache rule.resolverule from logresolverule rule,param_itemtype type,param_itemname iname where "
								+ "type.itemtype=\"" + name[1] + "\" and iname.itemname=\"" + productor
								+ "\" and rule.logname=\"" + logname
								+ "\" and rule.itemnameid=iname.itemnameid and rule.itemtypeid=type.itemtypeid"
								+ " limit 1");
				List<String> ruleIds = dbDao.selectPool(sql.toString(), keyRule);
				sql.delete(0, sql.length());
				if (ruleIds != null && ruleIds.size() != 0) {
					ruleId = ruleIds.get(0);
				} else {
					System.out.println(fullPath + ":" + fileName + "在数据表logresolverule查不到相应的解析规则");
					LOG.info(fullPath + ":" + fileName + "在数据表logresolverule查不到相应的解析规则");
					return false;
				}
				preProductor = productor;
				preRule = ruleId;
				preItemtype = name[1];
				preLogName = logname;
			} else
				ruleId = preRule;
//			queueTmp.setResolveruleid(ruleId);
			queueTmp.setResolverule(ruleId);
			time = to14(name[5].substring(name[5].lastIndexOf(".") + 1));
			rowKey.append(devName + "+" + name[1] + "+" + productor + "+" + name[3] + "+" + name[4] + "+" + logname
					+ "+" + time);
			queueTmp.setDevid(devId);
			queueTmp.setLog_id(rowKey.toString());
			queueTmp.setDevname(devName);
			queueTmp.setItemtype(name[1]);
			queueTmp.setItemname(productor);
			queueTmp.setItemvar(name[3]);
			queueTmp.setInstance(name[4]);
			queueTmp.setLogname(logname);
			queueTmp.setCollect_time(time); // 解析
			queueTmps.add(queueTmp);
			pathName.put(fullPath, rowKey.toString());
		} else {
			System.out.println(fullPath + "的文件名不符合规范");
			LOG.info(fullPath + "的文件名不符合规范");
		}
		return flag;
	}

	//性能数据解析
	public boolean performance(String fullPath) {
		int errorCount = 0;
		byte[] bs;
		String contents = "";
		String[] content = {};
		String[] splits = {};
		Map<String, String> perfinfo = new HashMap<String, String>();	//存储主机性能数据
		Map<String, String> nets = new HashMap<String, String>();		//存储网络性能数据
		Map<String, String> branchs = new HashMap<String, String>();	//存储二级行的性能数据
		HashSet<String> devIdItem = new HashSet<String>();//存储设备的指标项，最后存储入queue_sitem，CPU和MEM不变为传过来的指标项名称
														//DISK和FS的为指标项名称|磁盘号或者文件系统名
		String preDevName = "";
		String preDevId = "";
		String devId = "";
		String devName = "";
		boolean flagCpu = false;
		boolean flagBranch = false;
		boolean flagNet = false;
		boolean flag = false;
		Map<String, String> newData = new HashMap<String, String>();    //记录每台设备每个指标项的最新值最后存入Bean表中
		Map<String,Double> windowPageOut = null;            //window的pageOut需要单独处理
		String time = "";
		String[] keyIds = { "devid" };
		StringBuffer sql = new StringBuffer("select sql_cache devid from info_dev where devname=");
		try {
//			bs = FileUtils.toByteArray(fullPath);
			bs = HdfsUtil.readFromHdfs(fullPath);
			contents = new String(bs);
			content = contents.split("\n");
			for (String string : content) {
				splits = string.split("\\|");
				if (fullPath.contains("perfinfo.txt") && splits != null && splits.length == 7) {
					time = timeTo14(splits[4]);
					if (!splits[0].equals(preDevName)) {
						devName = splits[0];
						sql.append("\"" + devName + "\" limit 1");
						List<String> devIds = dbDao.selectPool(sql.toString(), keyIds);
						sql.delete(sql.lastIndexOf("=") + 1, sql.length());
						if (devIds != null && devIds.size() != 0)
							devId = devIds.get(0);
						else {
							if (devIds == null){
								System.out.println("连接数据库失败！！！");
								LOG.info("连接数据库失败！！！");
							}
							else{
								errorCount ++;
								System.out.println(fullPath + ":" + string + "中的" + splits[0] + "在数据表info_dev中查不到相应的设备记录");
								LOG.info(fullPath + ":" + string + "中的" + splits[0] + "在数据表info_dev中查不到相应的设备记录");
								continue;
							}
						}
					} else
						devId = preDevId;
					String rowKey = "";
					String rowkeyMeMPageOut = "";   //window系统中检查Mem利用率数据是否>=pageOut_Peri时添加一条rowKey为MEMPageOut=1的数据
					String item = "";
					String value = "";              //window系统DISK繁忙率为100-当前空前率，此值为空表示值为当前值，不为空为此值
					//注：主机的性能日志的指标项非以下指标项直接丢弃掉
					//非Window系统的性能指标统计四大项：CPU,MEM,DISK,FS
					//	rowKey的设计中CPU和MEMORY中的rowkey为三项中间用+号分割
					//	DISK和FS的rowKey为四项中间用+号分割
					//window系统的性能指标统计三大项：
					//	因所传window的性能指标名称与非window系统不同，最简单的方法是与前边四项的名称做个映射
					//	ProcessorUtilization映射为CPUCpuUtil
					//	MemoryUsage仍为MemoryUsage但判断其值是否大于等于total_Prei,是的话添加MEMPageOut指标项为1
					//	LDldFreeSpacePercent映射为DSKPercentBusy，值为100-当前值
					if (splits[1].equals("CPU") || splits[1].equals("MEMORY")) {
						rowKey = time + "+" + devId + "+" + splits[3];
						item = splits[3];
					} else if (splits[1].equals("FILESYSTEM") || splits[1].equals("DISK")) {
						rowKey = time + "+" + devId + "+" + splits[3] + "+" + splits[2];
						item = splits[3] + "|" +splits[2];
					} else if(splits[1].equals("NT_HEALTH")&&splits[2].equals("NT_HEALTH")){
						if(splits[3].equals("ProcessorUtilization")) {
							rowKey = time + "+" + devId + "+" + window_cpu_item;
							item = window_cpu_item;
						} else if (splits[3].equals("MemoryUsage")) {
							rowKey = time + "+" + devId + "+" + splits[3];
							if(Float.valueOf(splits[5]) >= pageOut_Peri)
							rowkeyMeMPageOut = time + "+" + devId + "+" + window_MemPageOut_item;
							item = splits[3];
						}
					}else if (splits[1].equals("NT_LOGICAL_DISKS")&&!splits[2].equals("_Total")) {
						if(splits[3].equals("LDldFreeSpacePercent")){
							rowKey = time + "+" + devId + "+" + window_disk_item + "+" + splits[2];
							item = window_disk_item + "|" +splits[2];
							value = String.valueOf(total_peri - Float.valueOf(splits[5]));
						}
					}
					if(rowKey!= null && !"".equals(rowKey)){
						perfinfo.put(rowKey, "".equals(value)?splits[5]:value);
						//第一行为widow系统的指标项的最新值添加进容器
						//第二行为window系统的指标项
						if(splits[3].equals("CPUCpuUtil") || splits[3].equals("MEMPageOut") || splits[3].equals("FSCapacity") || splits[3].equals("DSKPercentBusy")
								|| item.equals("CPUCpuUtil") || item.startsWith("DSKPercentBusy") || item.equals("MemoryUsage"))
						newData.put(rowKey, "".equals(value)?splits[5]:value);
					}
					windowPageOut = new HashMap<>();
					if(rowkeyMeMPageOut != null && !"".equals(rowkeyMeMPageOut)) {
						Double valuePageOut = windowPageOut.get(rowkeyMeMPageOut);
						windowPageOut.put(rowkeyMeMPageOut,valuePageOut!=null?(valuePageOut+1):1);
						devIdItem.add(devId + "+" + window_MemPageOut_item);
					}
					preDevName = splits[0];
					if (true && !"".equals(item))
						devIdItem.add(devId + "+" + item); // 添加到sitem数据表中的数据项
				} else if (fullPath.contains(netsmart) || fullPath.contains("branchsmart.txt")) {
					time = timeTo14(splits[3]);
					if (!splits[0].equals(preDevName)) {
						devName = splits[0];
						sql.append("\"" + devName + "\" limit 1");
						List<String> devIds = dbDao.selectPool(sql.toString(), keyIds);
						sql.delete(sql.lastIndexOf("=") + 1, sql.length());
						if (devIds != null && devIds.size() != 0)
							devId = devIds.get(0);
						else {
							if (devIds == null){
								System.out.println("连接数据库失败！！！");
								LOG.info("连接数据库失败！！！");
							}
							else {
//								devId = dbDao.addPool(devName);
								errorCount ++;
								System.out.println(
										fullPath + ":" + string + "中的" + splits[0] + "在数据表info_dev中查不到相应的设备记录");
								LOG.info(fullPath + ":" + string + "中的" + splits[0] + "在数据表info_dev中查不到相应的设备记录");
								continue;
							}
						}
					} else
						devId = preDevId;
					String rowkey = "";
					String item = "";
					
					
					if (true)
						devIdItem.add(devId + "+" + splits[1]); // 添加到sitem数据表中的数据项
					if (fullPath.contains(netsmart)){
						if (splits[1].equalsIgnoreCase("Cpu5min")||splits[1].equals("sysUpTime")||splits[1].equals("SystemMemoryUtil")||splits[1].equals("ciscomemUtilization[Processor]")
								||splits[1].contains("Temperature")||splits[1].contains("jnxOperatingCPU")||splits[1].contains("jnxoperatingmem")||splits[1].contains("jnxOperatingTemp")
								||splits[1].contains("nsResMemUtil")||splits[1].equals("nsResSessAllocate")) {
							if (splits[1].contains("Temperature")) {
								splits[1] = "Temperature";
							}
							rowkey = time + "+" + devId + "+" + splits[1];
							item = splits[1];							
						}
						if(rowkey!= null && !"".equals(rowkey)){
							newData.put(rowkey, splits[2]);
						}					
						nets.put(time + "+" + devId + "+" + splits[1], splits[2]);
					}else
						branchs.put(time + "+" + devId + "+" + splits[1], splits[2]);
					preDevName = splits[0];
				} else {
					errorCount ++;
					System.out.println(fullPath + ": " + string + "格式不符合要求");
					LOG.info(fullPath + ": " + string + "格式不符合要求");
				}
				preDevId = devId;
			}

			if (perfinfo.size() != 0){
				flagCpu = hBaseDao.insertBatch(perfinfo, "PerfUtility", "Info", "Utility") &&
						dbDao.addLocalNewDataPool(newData) && (windowPageOut != null ? dbDao.addLocalWindowPageOutPool(windowPageOut) : true);
				
			} else if (nets.size() != 0){
				flagNet = hBaseDao.insertBatch(nets, "NetUtility", "Info", "Utility")&&
						dbDao.addLocalNewDataPool(newData);
				}
			else if (branchs.size() != 0)
				flagBranch = hBaseDao.insertBatch(branchs, "branchsmart", "Info", "Utility");
			if (devIdItem != null && devIdItem.size() != 0)
				dbDao.addLocalsItemPool(devIdItem);
			if (flagCpu || flagBranch || flagNet) {
//				if (errorCount == 0 && HdfsUtil.rmFile(fullPath))
//					flag = true;
			}
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error(e.toString());
		}
		return flag;
	}

	public String to14(String time) {
		Long timeStamp = new Long(time);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		return sdf.format(new Date(Long.parseLong(String.valueOf(timeStamp))));
	}

	public String timeTo14(String time) {
		SimpleDateFormat sdfFrom = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat sdfTo = new SimpleDateFormat("yyyyMMddHHmmss");
		Date date = null;
		try {
			date = sdfFrom.parse(time);
		} catch (ParseException e) {
			e.printStackTrace();
			LOG.error(e.toString());
		}
		return sdfTo.format(date);
	}

	public String timeTo143(String time) {
		SimpleDateFormat sdfFrom = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
		SimpleDateFormat sdfTo = new SimpleDateFormat("yyyyMMddHHmmss");
		Date date = null;
		try {
			date = sdfFrom.parse(time);
		} catch (ParseException e) {
			e.printStackTrace();
			LOG.error(e.toString());
		}
		return sdfTo.format(date);
	}

	public boolean timeRange(String time) {
		boolean flag = false;
		String timeHHmmss = time.substring(8);
		if (timeHHmmss.substring(0, 2).equals("10")) {
			int mm = Integer.valueOf(timeHHmmss.substring(2, 4));
			if (mm >= 50 && mm <= 59)
				flag = true;
		}
		return flag;
	}

	public String toValue(String content) {
		int index = content.indexOf("{master}");
		String contents = null;
		if (index != -1)
			contents = content.substring(content.lastIndexOf("*****") + 5, content.indexOf("{master}"));
		return contents;
	}

	//配置文件分两种：1.文件名以localhost开头的为其他配置文件只需要解析出来存入hbase中即可
	//			 2.文件名以cacti开头的为需要进行MD5值比对的配置文件，对比mysql中如没有此设备，添加；如有此设备，且MD5与最近一条数据相同，不做修改；如有此设备，MD5与最新的MD5不同则添加
	public boolean performancesss(String fullPath) {
		int errorCount = 0;
		boolean flag = false;
		byte[] bs;
		String contents = "";
		String[] content = {};
		String preIpString = "";
		String ipString = "";
		String preDevId = "";
		Map<String, String> configMap = new HashMap<String, String>();
		Map<String, String> configMapTemp = new HashMap<String, String>();
		try {
//			bs = FileUtils.toByteArray(fullPath);
			bs = HdfsUtil.readFromHdfs(fullPath);
			contents = new String(bs);
			content = contents.split("\\|");
			StringBuffer sql = new StringBuffer();
			String pattern = "(\\*{0,})(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})(\\*{1,})(.*?)(\\*{1,})(\\d{8}\\s\\d{2}:\\d{2}:\\d{2}|\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2})";// (\\*{1,})(.*)";
			String pattern1 = "(\\*{0,})(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})(\\*{1,})(.*?)(\\*{1,})(\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2})(\\*{1,})(.*)(\\*{1,})";
			Pattern p = Pattern.compile(pattern);
			Pattern p1 = Pattern.compile(pattern1, Pattern.MULTILINE | Pattern.DOTALL);
			for (int i = 0; i < content.length; i++) {
				int index = content[i].trim().lastIndexOf("*****");
				if (index != -1) {
					String string = content[i].trim().substring(0, index);
					Matcher matcher = p.matcher(string);
					if (matcher.matches()) {
						String flagString = matcher.group(4);
						if (flagString != null && flagString.equals("show configuration")) {
							String value = toValue(content[i].trim());
							if (value == null || value == "")
								continue;
							ipString = matcher.group(2);
							String time = timeTo143(matcher.group(6));
							sql.append("select sql_cache devid from info_dev where devip=\"" + ipString + "\" limit 1");
							List<String> devIds = dbDao.selectPool(sql.toString(), new String[] { "devid" });
							String devId = null;
							sql.delete(0, sql.length());
							if (devIds != null && devIds.size() != 0) {
								devId = devIds.get(0);
							} else {
								errorCount ++;
								System.out.println(fullPath + ":" + ipString + "在数据表info_dev中查不到相应的设备记录");
								LOG.info(fullPath + ":" + ipString + "在数据表info_dev中查不到相应的设备记录");
							}
							String rowKey = devId + "+" + time;
							configMapTemp.put(rowKey, value);
							i += 3;
						}

					} else {
						String[] configurations = string.split("End\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*");
						if (configurations != null && configurations.length != 0) {
							for (int j = 0; j < configurations.length; j++) {
								if (j == 0)
									i += 3;
								Matcher matcher1 = p1.matcher(configurations[j].trim());
								if (matcher1.matches()) {
									String flagString = matcher1.group(4);
									String value = matcher1.group(8);
									if (value == null || value == "")
										continue;
									ipString = matcher1.group(2);
									String time = timeTo14(matcher1.group(6));
									String devId = null;
									if (!preIpString.equals(ipString)) {
										sql.append("select sql_cache devid from info_dev where devip=\"" + ipString
												+ "\" limit 1");
										List<String> devIds = dbDao.selectPool(sql.toString(),
												new String[] { "devid" });
										sql.delete(0, sql.length());
										if (devIds != null && devIds.size() != 0) {
											devId = devIds.get(0);
										} else {
											errorCount ++;
											System.out.println(fullPath + ":" + ipString + "在数据表info_dev中查不到相应的设备记录");
											LOG.info(fullPath + ":" + ipString + "在数据表info_dev中查不到相应的设备记录");
										}
										preIpString = ipString;
										preDevId = devId;
									} else
										devId = preDevId;
									String rowKey = devId + "+" + flagString.substring(flagString.indexOf("show") + 5)
											+ "+" + time;
									configMap.put(rowKey, value.substring(0, value.indexOf("***********")).trim());
								}
							}
						}
					}
				}
			}
			if (configMapTemp != null && configMapTemp.size() != 0) {
				NetDao netDao = new NetDaoImpl();
				for (String string2 : configMapTemp.keySet()) {
					String[] keys = string2.split("\\+");
					String devId = keys[0];
					int devid = Integer.parseInt(devId);
					String trans_time = keys[1];
					String value = configMapTemp.get(string2);
					String md5 = Md5Util.getMd5(value);
					RsNet net = new RsNet();
					boolean b1 = netDao.findDevid(devid);
					if (b1 == true) {
						net = netDao.findByDevid(devid);
						if (!net.getMd5mark().equals(md5)) {
							net.setMd5mark(md5);
							net.setTrans_time(trans_time);
							boolean f = netDao.insertRsNet(net);
							if (f == true) {
//								System.out.println("md5新增数据成功");
							} else {
								System.out.println("md5新增数据失败");
								LOG.info("md5新增数据失败");
							}
							String rowKey = devId + "+" + trans_time + "+" + md5;
							configMap.put(rowKey, value);
						}
					} else {
						net.setDevid(devid);
						net.setMd5mark(md5);
						net.setTrans_time(trans_time);
						boolean b3 = netDao.insertRsNet(net);
						if (b3 == true) {
//							System.out.println("md5新增数据成功");
						} else {
							System.out.println("md5新增数据失败");
							LOG.info("md5新增数据失败");
						}
						String rowKey = devId + "+" + trans_time + "+" + md5;
						configMap.put(rowKey, value);
					}
				}
			}
			if (configMap != null && configMap.size() != 0) {
				if (fullPath.contains("cacti"))
					flag = hBaseDao.insertBatch(configMap, "config", "info", "value");
				else {
					flag = hBaseDao.insertBatch(configMap, "configOther", "info", "value");
				}
			}
			if (flag) {
					if (errorCount == 0 && HdfsUtil.rmFile(fullPath))
						flag = true;
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e.toString());
		}
		return flag;
	}
	
	public static void main(String[] args) throws ParseException {
		Test test = new Test();
//		test.performance("C:\\Users\\Sherry\\Desktop\\shuju\\HNDB4510+APP+CMPB+1.0+0+perfinfo.txt.1465874906121");
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		System.out.println("开始时间："+format.format(new Date()));
		LOG.info("开始时间："+format.format(new Date()));
		test.fullFile("/Flume");
		System.out.println("结束时间："+format.format(new Date()));
		LOG.info("结束时间："+format.format(new Date())+"\r\n");
		System.out.println();
	}
}
