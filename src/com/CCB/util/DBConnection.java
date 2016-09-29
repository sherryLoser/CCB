package com.CCB.util;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.mysql.jdbc.PreparedStatement;

/**
 * @author Sherry
 *
 * @param <T>
 */
public class DBConnection {

	private static Connection conn = null;
	private static String JDriver = null;
	private static String url = null;
	private static String user = null;
	private static String password = null;
	private static Properties db_config= null;
	private static PreparedStatement statement = null;
	private static ResultSet resultSet = null;
	//private static ConnectionPool connPool = null;
	
	static {
		InputStream ins = DBConnection.class.getClassLoader().getResourceAsStream("DB.properties");
		db_config = new Properties();
		try {
			db_config.load(ins);
			JDriver = db_config.getProperty("DB_Driver");
			url = db_config.getProperty("DB_URL");
			user = db_config.getProperty("user");
			password = db_config.getProperty("password");
			// 创建数据库连接库对象
			//connPool = new ConnectionPool(JDriver, url, user, password);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static List<String> select(String sql,String key,Boolean flag){
		Connection conn = null;
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		conn = getConnection();
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

	public synchronized static Connection getConnection() {
		try {
			Class.forName(JDriver);
		} catch (ClassNotFoundException e) {
			e.getStackTrace();
		}
		try {
			conn = DriverManager.getConnection(url, user, password);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("数据库加载失败");
			System.exit(0);
		}
		System.out.println("加载数据库成功");
		return conn;
	}

	public synchronized static void closeConection(Connection conn, Statement st, ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
				rs = null;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (st != null) {
			try {
				st.close();
				st = null;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (conn != null) {
			try {
				conn.close();
				conn = null;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		System.out.println("关闭成功");
	}
	
	// delete,updata,insert
	public static boolean doSQL(String sql) {
		conn = DBConnection.getConnection();
		try {
			statement = (PreparedStatement) conn.prepareStatement(sql);
			statement.executeUpdate();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeConection(conn, statement, resultSet);
		}
		return false;
	}
}
