package com.CCB.bean;

import org.apache.solr.client.solrj.beans.Field;

//HNAP0200+OS+AIX+7100-02+0+cronlog+2016-04-15 14:10:54
public class LogBean {

	@Field
	String id;
	@Field
	String devname;
	@Field
	String itemtype;
	@Field
	String itemname;
	@Field
	String itemver;
	@Field
	String instance;
	@Field
	String logname;
	@Field
	String date;
	@Field
	String log;

	public String getDateTime() {
		return dateTime;
	}
	public void setDateTime(String dateTime) {
		this.dateTime = dateTime;
	}

	String dateTime;
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getDevname() {
		return devname;
	}

	public void setDevname(String devname) {
		this.devname = devname;
	}

	public String getItemtype() {
		return itemtype;
	}

	public void setItemtype(String itemtype) {
		this.itemtype = itemtype;
	}

	public String getItemname() {
		return itemname;
	}

	public void setItemname(String itemname) {
		this.itemname = itemname;
	}

	public String getItemver() {
		return itemver;
	}

	public void setItemver(String itemver) {
		this.itemver = itemver;
	}

	public String getInstance() {
		return instance;
	}

	public void setInstance(String instance) {
		this.instance = instance;
	}

	public String getLogname() {
		return logname;
	}

	public void setLogname(String logname) {
		this.logname = logname;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getLog() {
		return log;
	}

	public void setLog(String log) {
		this.log = log;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
