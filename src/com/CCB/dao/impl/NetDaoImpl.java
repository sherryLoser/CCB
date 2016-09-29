package com.CCB.dao.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.CCB.bean.RsNet;
import com.CCB.dao.AppLog;
import com.CCB.dao.NetDao;
import com.CCB.util.ConnectionPool;
import com.mysql.jdbc.PreparedStatement;

public class NetDaoImpl implements NetDao,AppLog {
	static ConnectionPool connPool = ConnectionPool.getInstance();
	private Connection conn;
	private Statement stmt;
	private PreparedStatement st;
	private ResultSet rs;

	public boolean findDevid(int devid) throws SQLException {
		boolean flag = false;
		
		String sql = "select * from rs_net_md5 where devid='" + devid
				+ "' order by trans_time desc limit 1 ";
		conn = connPool.getConnection();
		stmt = conn.createStatement();
		rs = stmt.executeQuery(sql);
		if (rs.next()) {
			RsNet net = new RsNet();
			net.setId(rs.getInt("id"));
			net.setDevid(rs.getInt("devid"));
			net.setMd5mark(rs.getString("md5mark"));
			net.setTrans_time(rs.getString("trans_time"));
			System.out.println("根据devid查询结果为:" + net);
			flag = true;
			// 记得关闭连接
			rs.close();
			stmt.close();
			connPool.returnConnection(conn);// 连接使用完后释放连接到连接池
		} else {
			// 记得关闭连接
			rs.close();
			stmt.close();
			connPool.returnConnection(conn);// 连接使用完后释放连接到连接池
		}
		return flag;
	}

	public RsNet findByDevid(int devid) throws SQLException {
		ConnectionPool connPool = ConnectionPool.getInstance();
		String sql = "select * from rs_net_md5 where devid='" + devid
				+ "' order by trans_time desc limit 1 ";
		conn = connPool.getConnection();
		stmt = conn.createStatement();
		rs = stmt.executeQuery(sql);
		if (rs.next()) {
			RsNet net = new RsNet();
			net.setId(rs.getInt("id"));
			net.setDevid(rs.getInt("devid"));
			net.setMd5mark(rs.getString("md5mark"));
			net.setTrans_time(rs.getString("trans_time"));
			System.out.println("根据devid查询结果为:" + net);
			// 记得关闭连接
			rs.close();
			stmt.close();
			connPool.returnConnection(conn);// 连接使用完后释放连接到连接池
			return net;
		} else {
			System.out.println("查询失败");
		}
		return null;
	}

	public boolean updateRsNet(int id, String trans_time) throws SQLException {
		boolean b = false;
		ConnectionPool connPool = ConnectionPool.getInstance();
		String sql = "update  rs_net_md5 set trans_time='" + trans_time
				+ "'where id=" + id;
		try {
			conn = connPool.getConnection();
			stmt = conn.createStatement();
			int i = stmt.executeUpdate(sql);
			if (i > 0) {
				b = true;
			} else {
				b = false;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			LOG.error(e.toString());
		}finally{
			stmt.close();
			connPool.returnConnection(conn);// 连接使用完后释放连接到连接池
		}

		return b;
	}

	public boolean insertRsNet(RsNet net) {
		boolean flag = false;
		ConnectionPool connPool = ConnectionPool.getInstance();
		String sql = "insert into  rs_net_md5(devid,md5mark,trans_time) values(?,?,?)";
		try {
			Connection conn = connPool.getConnection(); // 从连接库中获取一个可用的连接
			st = (PreparedStatement) conn.prepareStatement(sql.toString());
			st.setInt(1, net.getDevid());
			st.setString(2, net.getMd5mark());
			st.setString(3, net.getTrans_time());

			int i = st.executeUpdate();
			if (i > 0) {
				flag = true;
			} else {
				flag = false;
			}
			st.close();
			connPool.returnConnection(conn);// 连接使用完后释放连接到连接池
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

		return flag;

	}
	
	public static void main(String[] args) throws Exception {
		NetDao netDao = new NetDaoImpl();
		int devid = 100126;
		String trans_time = "20160609010001";
		String md5 = "661612e187a5940c6f3a0fefeda7b62f";
		RsNet net = new RsNet();
		connPool.createPool();
		
			net = netDao.findByDevid(devid);
			if(net != null){
			System.out.println(net);
			if (net.getMd5mark().equals(md5)) {
				System.out.println("md5Mark字段匹配");
//				int id = net.getId();
//				boolean b2 = netDao.updateRsNet(id, trans_time);
//				if (b2 == true) {
//					System.out.println("修改trans_time成功");
//				} else {
//					System.out.println("修改trans_time失败");
//				}
			} else {
				System.out.println("md5mark字段不匹配");
				net.setMd5mark(md5);
				net.setTrans_time(trans_time);
				boolean f = netDao.insertRsNet(net);
				if (f == true) {
					System.out.println("新增数据成功");
				} else {
					System.out.println("新增数据失败");
				}
				System.out.println(net.getMd5mark());
			}
		} else {
			System.out.println("数据库中无对应devid，即将增加一行新数据");
			net.setDevid(devid);
			net.setMd5mark(md5);
			net.setTrans_time(trans_time);
			boolean b3 = netDao.insertRsNet(net);
			if (b3 == true) {
				System.out.println("新增数据成功");
			} else {
				System.out.println("新增数据失败");
			}
			System.out.println(net.getMd5mark());
		}
	}

}
