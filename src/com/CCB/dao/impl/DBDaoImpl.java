package com.CCB.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.CCB.bean.QueueTmp;
import com.CCB.dao.AppLog;
import com.CCB.dao.DBInter;
import com.CCB.util.ConnectionPool;
import com.CCB.util.DBConnection;

public class DBDaoImpl implements DBInter,AppLog {
	static ConnectionPool connPool = ConnectionPool.getInstance();
	Statement stmt = null;
	ResultSet rs = null;
	PreparedStatement ps = null;
	Connection conn = null;
	public List<String> selectPool(String sql, String[] keys) {
		List<String> list = new ArrayList<String>();
		try {
			// 新建数据库连接库
			connPool.createPool();
			conn = connPool.getConnection(); // 从连接库中获取一个可用的连接
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				for (String key : keys) {
					list.add(rs.getString(key));
				}
			}
//			rs.close();
//			stmt.close();
//			connPool.returnConnection(conn);// 连接使用完后释放连接到连接池
			// connPool.refreshConnections();//刷新数据库连接池中所有连接，即不管连接是否正在运行，都把所有连接都释放并放回到连接池。注意：这个耗时比较大。
			// connPool.closeConnectionPool();// 关闭数据库连接池。注意：这个耗时比较大。
		} catch (SQLException e) {
			e.printStackTrace();
			LOG.error(e.toString());
			return null;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			LOG.error(e.toString());
			return null;
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e.toString());
			return null;
		}finally {
			close(conn, ps, stmt, rs);
		}
		return list;
	}
	
	public boolean addHbaseQueueLocalPool(List<QueueTmp> queueTmps) {
		boolean flag = false;
		try {
			// 新建数据库连接库
			connPool.createPool();
			StringBuffer sql = new StringBuffer("insert IGNORE into hbase_queue");
			sql.append("(log_id,resolveruleid,devname,devid,itemtype,itemname,instance,logname,collect_time,itemver,resolverule)");
			sql.append(" values (?,?,?,?,?,?,?,?,?,?,?)");
			conn = connPool.getConnection();
			conn.setAutoCommit(false);
			ps = (PreparedStatement) conn.prepareStatement(sql.toString());
			int i = 0;
			for (QueueTmp queueTmp : queueTmps) {
				ps.setString(1, queueTmp.getLog_id());
				ps.setString(2, queueTmp.getResolveruleid());
				ps.setString(3, queueTmp.getDevname());
				ps.setString(4, queueTmp.getDevid());
				ps.setString(5, queueTmp.getItemtype());
				ps.setString(6, queueTmp.getItemname());
				ps.setString(7, queueTmp.getInstance());
				ps.setString(8, queueTmp.getLogname());
				ps.setString(9, queueTmp.getCollect_time());
				ps.setString(10, queueTmp.getItemvar());
				ps.setString(11, queueTmp.getResolverule());
				i++;
				ps.addBatch();
				if (i != 0 && i % 1000 == 0)
					ps.executeBatch();
			}
			ps.executeBatch();
			conn.commit();
			conn.setAutoCommit(true);
			flag = true;
			// connPool.refreshConnections();//刷新数据库连接池中所有连接，即不管连接是否正在运行，都把所有连接都释放并放回到连接池。注意：这个耗时比较大。
			// connPool.closeConnectionPool();// 关闭数据库连接池。注意：这个耗时比较大。
		} catch (SQLException e) {
			e.printStackTrace();
			LOG.error(e.toString());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			LOG.error(e.toString());
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e.toString());
		}finally {
			close(conn, ps, stmt, rs);
		}
		return flag;
	}

	public boolean addHbaseTmpLocalPool(List<QueueTmp> queueTmps) {
		boolean flag = false;
		try {
			// 新建数据库连接库
			connPool.createPool();
			StringBuffer sql = new StringBuffer("insert IGNORE into hbase_queue_tmp");
			sql.append("(log_id,resolveruleid,devname,devid,itemtype,itemname,instance,logname,collect_time,itemver,flag,resolverule)");
			sql.append(" values (?,?,?,?,?,?,?,?,?,?,?,?)");
			conn = connPool.getConnection();
			conn.setAutoCommit(false);
			ps = (PreparedStatement) conn.prepareStatement(sql.toString());
			int i = 0;
			for (QueueTmp queueTmp : queueTmps) {
				ps.setString(1, queueTmp.getLog_id());
				ps.setString(2, queueTmp.getResolveruleid());
				ps.setString(3, queueTmp.getDevname());
				ps.setString(4, queueTmp.getDevid());
				ps.setString(5, queueTmp.getItemtype());
				ps.setString(6, queueTmp.getItemname());
				ps.setString(7, queueTmp.getInstance());
				ps.setString(8, queueTmp.getLogname());
				ps.setString(9, queueTmp.getCollect_time());
				ps.setString(10, queueTmp.getItemvar());
				ps.setInt(11, 0);
				ps.setString(12, queueTmp.getResolverule());
				i++;
				ps.addBatch();
				if (i != 0 && i % 1000 == 0)
					ps.executeBatch();
			}
			ps.executeBatch();
			conn.commit();
			conn.setAutoCommit(true);
			flag = true;
			// connPool.refreshConnections();//刷新数据库连接池中所有连接，即不管连接是否正在运行，都把所有连接都释放并放回到连接池。注意：这个耗时比较大。
			// connPool.closeConnectionPool();// 关闭数据库连接池。注意：这个耗时比较大。
		} catch (SQLException e) {
			e.printStackTrace();
			LOG.error(e.toString());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			LOG.error(e.toString());
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e.toString());
		}finally {
			close(conn, ps, stmt, rs);
		}
		return flag;
	}

	public boolean addLocalsItemPool(HashSet<String> keys) {
		boolean flag = false;
		try {
			// 新建数据库连接库
			connPool.createPool();
			StringBuffer sql = new StringBuffer("insert IGNORE into queue_sitem" + "(devid,sitem)" + " values (?,?)");
			conn = connPool.getConnection();
			conn.setAutoCommit(false);
			ps = (PreparedStatement) conn.prepareStatement(sql.toString());
			int i = 0;
			for (String key : keys) {
				String[] devIdItem = key.split("\\+");
				if (devIdItem[0].isEmpty()){
					continue;
				}
				ps.setInt(1, Integer.valueOf(devIdItem[0]));
				ps.setString(2, devIdItem[1]);
				i++;
				ps.addBatch();
				if (i != 0 && i % 1000 == 0)
					ps.executeBatch();
			}
			ps.executeBatch();
			conn.commit();
//			ps.close();
//			connPool.returnConnection(conn);// 连接使用完后释放连接到连接池
			flag = true;
			// connPool.refreshConnections();//刷新数据库连接池中所有连接，即不管连接是否正在运行，都把所有连接都释放并放回到连接池。注意：这个耗时比较大。
			// connPool.closeConnectionPool();// 关闭数据库连接池。注意：这个耗时比较大。
		} catch (SQLException e) {
			e.printStackTrace();
			LOG.error(e.toString());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			LOG.error(e.toString());
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e.toString());
		}finally {
			close(conn, ps, stmt, rs);
		}
		return flag;
	}

	public boolean addLocalNewDataPool(Map<String, String> newDatas) {
		boolean flag = false;
		try {
			// 新建数据库连接库
			connPool.createPool();
			StringBuffer sql = new StringBuffer(
					"replace into Bean (devId,itemTime,item,flagItem,valueItem) values (?,?,?,?,?)");
			conn = connPool.getConnection();
			conn.setAutoCommit(false);
			ps = (PreparedStatement) conn.prepareStatement(sql.toString());
			int i = 0;
			for (String key : newDatas.keySet()) {
				String[] newData = key.split("\\+");
				if (newData != null && newData.length >= 3) {
					ps.setString(1, newData[1]);
					ps.setString(2, newData[0]);
					ps.setString(3, newData[2]);
					ps.setString(4, newData.length == 4 ? newData[3] : "");
					ps.setString(5, newDatas.get(key));
					i++;
					ps.addBatch();
					if (i != 0 && i % 1000 == 0)
						ps.executeBatch();
				}
			}
			ps.executeBatch();
			conn.commit();
//			ps.close();
//			connPool.returnConnection(conn);// 连接使用完后释放连接到连接池
			flag = true;
			// connPool.refreshConnections();//刷新数据库连接池中所有连接，即不管连接是否正在运行，都把所有连接都释放并放回到连接池。注意：这个耗时比较大。
			// connPool.closeConnectionPool();// 关闭数据库连接池。注意：这个耗时比较大。
		} catch (SQLException e) {
			e.printStackTrace();
			LOG.error(e.toString());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			LOG.error(e.toString());
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e.toString());
		}finally {
			close(conn, ps, stmt, rs);
		}
		return flag;
	}

	public boolean addLocalWindowPageOutPool(Map<String, Double> windowPageOut){
		boolean flag = false;
		try {
			// 新建数据库连接库
			connPool.createPool();
			StringBuffer sql = new StringBuffer(
					"insert into Bean(devId,itemTime,item,flagItem,valueItem) values (?,?,?,?,?)"
					+ " on DUPLICATE KEY update itemTime = ?,valueItem = valueItem + ?");
			conn = connPool.getConnection();
			conn.setAutoCommit(false);
			ps = (PreparedStatement) conn.prepareStatement(sql.toString());
			int i = 0;
			for (String key : windowPageOut.keySet()) {
				String[] newData = key.split("\\+");
				if (newData != null && newData.length >= 3) {
					ps.setString(1, newData[1]);
					ps.setString(2, newData[0]);
					ps.setString(3, newData[2]);
					ps.setString(4, newData.length == 4 ? newData[3] : "");
					ps.setDouble(5, windowPageOut.get(key));
					ps.setString(6, newData[0]);
					ps.setDouble(7, windowPageOut.get(key));
					i++;
					ps.addBatch();
					if (i != 0 && i % 1000 == 0)
						ps.executeBatch();
				}
			}
			ps.executeBatch();
			conn.commit();
//			ps.close();
//			connPool.returnConnection(conn);// 连接使用完后释放连接到连接池
			flag = true;
			// connPool.refreshConnections();//刷新数据库连接池中所有连接，即不管连接是否正在运行，都把所有连接都释放并放回到连接池。注意：这个耗时比较大。
			// connPool.closeConnectionPool();// 关闭数据库连接池。注意：这个耗时比较大。
		} catch (SQLException e) {
			e.printStackTrace();
			LOG.error(e.toString());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			LOG.error(e.toString());
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error(e.toString());
		}finally {
			close(conn, ps, stmt, rs);
		}
		return flag;
	}
	
	public String addPool(String devname) {
		String id = "";
		try {
			// 新建数据库连接库
			connPool.createPool();
			StringBuffer sql = new StringBuffer("insert into info_dev (flag,devname,devip,devnote,productorid,typeid,state)"
					+ " values (0,?,'19.10.23.1.2','测试','10000','14',0)");
			conn = connPool.getConnection();
			PreparedStatement ps = (PreparedStatement) conn.prepareStatement(sql.toString());
			ps.setString(1, devname);
			ps.execute();
			ps.close();

//			String sql2 = "select devid from info_dev where devname = " + devname;
//			stmt = conn.createStatement();
//			rs = stmt.executeQuery(sql2);
//
//			while (rs.next()) {
//				id = rs.getString("devid");
//			}
			rs.close();
			stmt.close();
			connPool.returnConnection(conn);// 连接使用完后释放连接到连接池
			// connPool.refreshConnections();//刷新数据库连接池中所有连接，即不管连接是否正在运行，都把所有连接都释放并放回到连接池。注意：这个耗时比较大。
			// connPool.closeConnectionPool();// 关闭数据库连接池。注意：这个耗时比较大。
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return id;
	}

	public static void main(String[] args) {
		Map<String, Double> ss = new HashMap<>();
		ss.put("333+200000+MEMPageOut",2.0);
		ss.put("333+300000+MEMPageOut",3.0);
		DBDaoImpl dbDaoImpl = new DBDaoImpl();
		System.out.println(dbDaoImpl.addLocalWindowPageOutPool(ss));
//		ConnectionPool connPool = ConnectionPool.getInstance();
//		try {
//			connPool.createPool();
//			Connection conn = connPool.getConnection();
//			long start = System.currentTimeMillis();
//			conn.setAutoCommit(false);
//			// 新建数据库连接库
//			StringBuffer sql = new StringBuffer("insert IGNORE into test" + "(name)" + " values (?)");
//			PreparedStatement ps = (PreparedStatement) conn.prepareStatement(sql.toString());
//			for (int i = 0; i < 50000; i++) {
//				ps.setString(1, i + "");
//				ps.addBatch();
//				if (i % 1000 == 0) {
//					ps.executeBatch();
//					// conn.commit();
//					ps.clearBatch();
//				}
//			}
//			ps.executeBatch();
//			conn.commit();
//			ps.close();
//			connPool.returnConnection(conn);// 连接使用完后释放连接到连接池
//			System.out.println(System.currentTimeMillis() - start);
//		} catch (SQLException e) {
//			e.printStackTrace();
//		} catch (ClassNotFoundException e) {
//			e.printStackTrace();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		// System.out.println(dbDaoImpl.addPool("123"));
	}

	public boolean addstategyInfoToLocalMySQL(List<QueueTmp> queueTmps) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		StringBuffer sql = new StringBuffer("insert into hbase_queue_tmp"
				+ "(log_id,resolveruleid,devname,devid,itemtype,itemname,instance,logname,collect_time,itemver,flag)"
				+ " values ");

		// 获取该数据源的所有表信息
		try {
			for (QueueTmp queueTmp : queueTmps) {
				sql.append("(\'" + queueTmp.getLog_id() + "\',\'" + queueTmp.getResolveruleid() + "\',\'"
						+ queueTmp.getDevname() + "\',\'" + queueTmp.getDevid() + "\',\'" + queueTmp.getItemtype()
						+ "\',\'" + queueTmp.getItemname() + "\',\'" + queueTmp.getInstance() + "\',\'"
						+ queueTmp.getLogname() + "\',\'" + queueTmp.getCollect_time() + "\',\'" + queueTmp.getItemvar()
						+ "\'," + 0 + "),");
			}
			String sqlString = sql.substring(0, sql.lastIndexOf(","));
			conn = DBConnection.getConnection();
			ps = (PreparedStatement) conn.prepareStatement(sqlString);
			ps.executeUpdate();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			DBConnection.closeConection(conn, ps, rs);
		}
		return true;
	}

	public boolean addToLocalMySQL(List<QueueTmp> queueTmps) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		StringBuffer sql = new StringBuffer("insert into hbase_queue_tmp"
				+ "(log_id,resolveruleid,devname,devid,itemtype,itemname,instance,logname,collect_time,itemver,flag)"
				+ " values (?,?,?,?,?,?,?,?,?,?,?)");

		// 获取该数据源的所有表信息
		try {
			conn = DBConnection.getConnection();
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(sql.toString());
			for (QueueTmp queueTmp : queueTmps) {
				ps.setString(1, queueTmp.getLog_id());
				ps.setString(2, queueTmp.getResolveruleid());
				ps.setString(3, queueTmp.getDevname());
				ps.setString(4, queueTmp.getDevid());
				ps.setString(5, queueTmp.getItemtype());
				ps.setString(6, queueTmp.getItemname());
				ps.setString(7, queueTmp.getInstance());
				ps.setString(8, queueTmp.getLogname());
				ps.setString(9, queueTmp.getCollect_time());
				ps.setString(10, queueTmp.getItemvar());
				ps.setInt(11, 0);
				ps.addBatch();
			}
			int[] i = ps.executeBatch();
			System.out.println(i.length);
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			DBConnection.closeConection(conn, ps, rs);
		}
		return true;
	}

	public List<String> select(String sql, String key) {
		Connection conn = null;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		conn = DBConnection.getConnection();
		List<String> list = new ArrayList<String>();
		try {
			statement = (PreparedStatement) conn.prepareStatement(sql);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				list.add(resultSet.getString(key));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeConection(conn, statement, resultSet);
		}
		return list;
	}
	
	private void close(Connection connection,PreparedStatement ps,Statement st,ResultSet rs){
			if(connection != null){
				connPool.returnConnection(connection);
			}
			if (rs != null) {
				try {
					rs.close();
					rs = null;
				} catch (SQLException e) {
					e.printStackTrace();
					LOG.error(e.toString());
				}
			}
			if (st != null) {
				try {
					st.close();
					st = null;
				} catch (SQLException e) {
					e.printStackTrace();
					LOG.error(e.toString());
				}
			}
	}
}
