package com.cstor.master.service;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cstor.slaves.bean.TaskNode;
import com.cstor.slaves.service.TaskNodeBasicService;
import com.cstor.utils.Properties;
import com.cstor.utils.SerializableUtil;

public class JMaster {

	private static final Logger LOG = LoggerFactory.getLogger(JMaster.class);
	private RPCServiceThread rpcServiceThread;
	private TaskNodeBasicService taskNodeBasicService;
	private MasterSyncThread masterSyncThread;

	private Properties properties;

	
	public void start(){
		//get properties, if null, stop
		LOG.info("Init System resource info.....");
		if(properties == null){
			LOG.error("......Init property file fail !!!!!.......");
			System.exit(-4);
		}
		
		ExecutorService exe = Executors.newCachedThreadPool();
		//start rpc service,default port 6666
//		this.rpcServiceThread.start();
		exe.execute(rpcServiceThread);
		
		TaskNode taskNode = new TaskNode(properties.getHostName());
		byte[] dataBuffer = SerializableUtil.writeObject(taskNode);
		taskNodeBasicService.regist(dataBuffer);
	
		exe.execute(masterSyncThread);
		JMasterProcess process = new JMasterProcess(taskNodeBasicService, properties);
		
		exe.execute(process);
		LOG.info("start process sucess...");
		
	}
	
	
	public JMaster(int port,Map<String, String> registHandle) {
		this.rpcServiceThread = new RPCServiceThread(port, registHandle);
		this.taskNodeBasicService = new TaskNodeBasicService();
		this.masterSyncThread = new MasterSyncThread();
		this.properties = Properties.getInstance();
		
	}
	
}
