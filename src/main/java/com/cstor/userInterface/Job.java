package com.cstor.userInterface;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Job implements Serializable{
	
	private static final long serialVersionUID = 2000L;
	private int id;
	private long createdTime;
	private long startTime;
	private long endTime;
	private String name;
	private String procHostName;
	private Configuration configuration;
	private int status;
	private int priority;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public long getCreatedTime() {
		return createdTime;
	}
	
	public String getFormatCreatedTime() {
	    return toDateStr(createdTime);
	}

	public void setCreatedTime(long createdTime) {
		this.createdTime = createdTime;
	}

	public long getStartTime() {
		return startTime;
	}
	
	public String getFormatStartTime() {
	    return toDateStr(startTime);
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getEndTime() {
		return endTime;
	}
	
	public String getFormatEndTime() {
	    return toDateStr(endTime);
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getProcHostName() {
		return procHostName;
	}

	public void setProcHostName(String procHostName) {
		this.procHostName = procHostName;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}
	
	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public int getPriority() {
		return priority;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}

	public Job(int id, long createdTime, long startTime, long endTime,String name, String procHostName, Configuration configuration) {
		this.id = id;
		this.createdTime = createdTime;
		this.startTime = startTime;
		this.endTime = endTime;
		this.name = name;
		this.procHostName = procHostName;
		this.configuration = configuration;
		this.status = Status.INIT;
	}

	public Job() {
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
//		private ConcurrentHashMap<String, Task> taskMap;
		sb.append("job -- > \n");
		sb.append("id           \t="+id+"\n");
		sb.append("createdTime  \t="+toDateStr(createdTime)+"\n");
		sb.append("startTime    \t="+toDateStr(startTime)+"\n");
		sb.append("endTime      \t="+toDateStr(endTime)+"\n");
		sb.append("endTime      \t="+toDateStr(endTime)+"\n");
		sb.append("name         \t="+name+"\n");
		sb.append("procHostName \t="+procHostName+"\n");
		sb.append("configuration\t="+configuration+"\n");
		sb.append("status       \t="+status+"\n");
		return sb.toString();
	}
	public String toDateStr(long time) {
		return toDateStr(time,"yyyy-MM-dd HH:mm:ss");
	}
	public String toDateStr(long time,String pattern){
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(time);
		return new SimpleDateFormat(pattern).format(cal.getTime());
	}

}
