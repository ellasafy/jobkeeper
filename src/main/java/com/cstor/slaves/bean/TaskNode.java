package com.cstor.slaves.bean;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class TaskNode implements Serializable{
	private static final long serialVersionUID = 1L;
	private String hostName;
	private long lastContent;
	private long startTime;
	private List<String> jobZkNameList;
	
	public String getHostName() {
		return hostName;
	}
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	public long getLastContent() {
		return lastContent;
	}
	public void setLastContent(long lastContent) {
		this.lastContent = lastContent;
	}
	public long getStartTime() {
		return startTime;
	}
	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}
	public List<String> getJobZkNameList() {
		return jobZkNameList;
	}
	public void setJobZkNameList(List<String> jobZkNameList) {
		this.jobZkNameList = jobZkNameList;
	}
	
	public TaskNode() {
		// TODO Auto-generated constructor stub
	}
	
	public TaskNode(String hostName){
		this.hostName = hostName;
		this.startTime = System.currentTimeMillis();
		this.lastContent = System.currentTimeMillis();
		this.jobZkNameList = new LinkedList<String>();
	}
	
	@Override
	public String toString() {
		return "TaskNode [hostName=" + hostName + ", lastContent="
				+ lastContent + ", startTime=" + startTime + ", jobZkNameList="
				+ jobZkNameList + "]";
	}
}
