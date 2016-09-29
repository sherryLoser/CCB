package com.CCB.bean;

public class RsNet {
	private int id ;
	private int devid;
	private String md5mark;
	private String trans_time;
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getDevid() {
		return devid;
	}
	public void setDevid(int devid) {
		this.devid = devid;
	}
	public String getMd5mark() {
		return md5mark;
	}
	public void setMd5mark(String md5mark) {
		this.md5mark = md5mark;
	}
	public String getTrans_time() {
		return trans_time;
	}
	public void setTrans_time(String trans_time) {
		this.trans_time = trans_time;
	}
	public RsNet(int id, int devid, String md5mark, String trans_time) {
		super();
		this.id = id;
		this.devid = devid;
		this.md5mark = md5mark;
		this.trans_time = trans_time;
	}
	public RsNet() {
		super();
	}
	@Override
	public String toString() {
		return "RsNet [id=" + id + ", devid=" + devid + ", md5mark=" + md5mark
				+ ", trans_time=" + trans_time + "]";
	}
}
