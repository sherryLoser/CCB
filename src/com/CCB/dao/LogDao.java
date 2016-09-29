package com.CCB.dao;


import java.util.List;
import java.util.UUID;

import com.CCB.bean.LogBean;
import com.CCB.util.date.DateFormat;
import com.CCB.util.solr.SolrUilts;


public class LogDao {

	
	public void creatIndex(List<LogBean> logBeans){
		SolrUilts.addBeans(logBeans);
	}
	
	public LogBean getLogBean(String rowKey,String content){
		LogBean logBean = new LogBean();
		String[] lb = rowKey.split("\\+");
		logBean.setDevname(lb[0]);
		logBean.setItemtype(lb[1]);
		logBean.setItemname(lb[2]);
		logBean.setItemver(lb[3]);
		logBean.setInstance(lb[4]);
		logBean.setLogname(lb[5]);
		logBean.setLog(content);
		logBean.setDate(DateFormat.getDateTime(lb[6]));
		logBean.setId(UUID.randomUUID().toString());
		return logBean;
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
