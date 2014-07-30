package com.cstor.master.bean;

import java.io.Serializable;

public class ServerStatus implements Serializable{
	private static final long serialVersionUID = 2335337998878503706L;
	// ip?°å?
	private String ip;
	// cpu ä½¿ç???
	private String cpuused;
	// ?»å?å­?
	private String totalmem;
	// ???ä½¿ç?
	private String memused;
	// ä¸????º¦
	private String uploadNetBand;
	// ä¸?½½??º¦
	private String downloadNetBand;
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getCpuused() {
		return cpuused;
	}
	public void setCpuused(String cpuused) {
		this.cpuused = cpuused;
	}
	public String getTotalmem() {
		return totalmem;
	}
	public void setTotalmem(String totalmem) {
		this.totalmem = totalmem;
	}
	public String getMemused() {
		return memused;
	}
	public void setMemused(String memused) {
		this.memused = memused;
	}
	public String getUploadNetBand() {
		return uploadNetBand;
	}
	public void setUploadNetBand(String uploadNetBand) {
		this.uploadNetBand = uploadNetBand;
	}
	public String getDownloadNetBand() {
		return downloadNetBand;
	}
	public void setDownloadNetBand(String downloadNetBand) {
		this.downloadNetBand = downloadNetBand;
	}
	@Override
	public String toString() {
		return "ServerStatus [ip=" + ip + ", cpuused=" + cpuused
				+ ", totalmem=" + totalmem + ", memused=" + memused
				+ ", uploadNetBand=" + uploadNetBand + ", downloadNetBand="
				+ downloadNetBand + "]";
	}

}
