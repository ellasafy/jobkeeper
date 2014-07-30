package com.cstor.slaves.service;

public class SystemInfoService {
	private MachineInfoService machineInfoService;
	public SystemInfoService() {
		this.machineInfoService = new MachineInfoService();
	}
	
	public double getCpuPer(){
		return machineInfoService.getCpuUsePer();
	}
	
	public double getMemPer(){
		return machineInfoService.getMemUsed() / machineInfoService.getMemAll();
	}
	
	public String getSpace()
	{
	    return machineInfoService.getSpaceInfo();
	}
	
	public boolean testConnection(){
		return true;
	}
	public String getServerState() {
		return machineInfoService.getServerState();
	}
	public double getMemUsePer()
	{
	    return machineInfoService.getMemUsed();
	}
	public double getMem()
	{
	    return machineInfoService.getMemAll();
	}
	// ?·å?è´?½½ä¿¡æ?
	public String getServerInfo() {
		return machineInfoService.getServerInfo();
	}
	
}
