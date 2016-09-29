package com.CCB.dao;

import java.sql.SQLException;

import com.CCB.bean.RsNet;

public interface NetDao {
	public boolean findDevid(int devid) throws SQLException;
	public RsNet findByDevid(int devid) throws SQLException;
	public boolean updateRsNet(int id,String trans_time) throws SQLException;
	public boolean insertRsNet(RsNet net);
}
