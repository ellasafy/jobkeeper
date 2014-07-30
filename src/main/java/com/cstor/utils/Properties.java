package com.cstor.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class Properties {

	private static Properties instance = null;

	private String connectString;
	private String hostName;
	private int sessionTimeout;
	private int connectionRetryCount;
	private int clientPoolCount;
	private int limitJobCount;
	private int bandwidth = 100 * 1000;
	
	public Properties(String connectString, String hostName, int sessionTimeout, 
			int connectionRetryCount, int clientPoolCount)
	{
		this.connectString = connectString;
		this.hostName  = hostName;
		this.sessionTimeout = sessionTimeout;
		this.connectionRetryCount = connectionRetryCount;
		this.clientPoolCount = clientPoolCount;
	}
	
	public String getConnectString() {
		return connectString;
	}

	public void setConnectString(String connectString) {
		this.connectString = connectString;
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public int getSessionTimeout() {
		return sessionTimeout;
	}

	public void setSessionTimeout(int sessionTimeout) {
		this.sessionTimeout = sessionTimeout;
	}

	public int getConnectionRetryCount() {
		return connectionRetryCount;
	}

	public void setConnectionRetryCount(int connectionRetryCount) {
		this.connectionRetryCount = connectionRetryCount;
	}

	public int getClientPoolCount() {
		return clientPoolCount;
	}

	public void setClientPoolCount(int clientPoolCount) {
		this.clientPoolCount = clientPoolCount;
	}

	public int getLimitJobCount() {
		return limitJobCount;
	}

	public void setLimitJobCount(int limitJobCount) {
		this.limitJobCount = limitJobCount;
	}

	public int getBandwidth() {
		return bandwidth;
	}

	public void setBandwidth(int bandwidth) {
		this.bandwidth = bandwidth;
	}

	private Properties() {
		String configPath = System.getProperty("JOBKEEPER.CONF");
		if(configPath != null){
			try {
				parse(configPath + File.separator + "core-site.cfg");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	
	public static Properties getInstance(){
		if(instance == null){
			instance = new Properties();
		}
		return instance;
	}
	
	public static Properties getInstance(String path){
		if(instance == null){
			instance = new Properties(path);
		}
		return instance;
	}
	
	private Properties(String path) {
		try {
			if(System.getProperty("JobKeeper.conf") == null){
				System.setProperty("JobKeeper.conf", path.endsWith(File.separator)?path.substring(0,path.length()-1):path);
			}
			parse(path + File.separator + "core-site.cfg");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void parse(String path) throws IOException {
		File configFile = new File(path);
		try {
			if (!configFile.exists()) {
				throw new IllegalArgumentException(configFile.toString()+ " file is missing");
			}
			java.util.Properties cfg = new java.util.Properties();
			FileInputStream in = new FileInputStream(configFile);
			try {
				cfg.load(in);
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				in.close();
			}
			parseProperties(cfg);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
	}

	private void parseProperties(java.util.Properties properties) {
		this.connectString = properties.getProperty("connectString");
		this.hostName = properties.getProperty("hostName");
		this.sessionTimeout = Integer.parseInt(properties.getProperty("sessionTimeout"));
		this.connectionRetryCount = Integer.parseInt(properties.getProperty("connectionRetryCount"));
		this.clientPoolCount = Integer.parseInt(properties.getProperty("clientPoolCount"));
		this.limitJobCount = Integer.parseInt(properties.getProperty("limitJobCount"));
	}



}
