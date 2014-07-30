package com.cstor.master.bean;

import java.io.Serializable;

public class Server implements Serializable{
	private static final long serialVersionUID = 1L;
	private String id;
	private String serverip;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getServerip() {
		return serverip;
	}
	public void setServerip(String serverip) {
		this.serverip = serverip;
	}
	
	public String toString() {
		return "Server [id=" + id + ", serverip=" + serverip + "]";
	}
}	
