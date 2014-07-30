package com.cstor.master.service;

import java.io.IOException;
import java.util.Map;
import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.server.PropertyHandlerMapping;
import org.apache.xmlrpc.server.XmlRpcServer;
import org.apache.xmlrpc.server.XmlRpcServerConfigImpl;
import org.apache.xmlrpc.webserver.WebServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RPCServiceThread implements Runnable {

	private static final Logger LOG = LoggerFactory.getLogger(RPCServiceThread.class);
	private int port;
	private Map<String, String> registHandle;
	private WebServer webServer;
	
	public  RPCServiceThread() {}
	
	private void startService(){
		try {
			LOG.info("Starting RPC Service please wait !!!....");
			this.webServer = new WebServer(port);
			LOG.info("Bind in port : " + port);
			XmlRpcServer rpcServer = webServer.getXmlRpcServer();
			PropertyHandlerMapping propertyHandlerMapping = new PropertyHandlerMapping();
			LOG.info("Classes are registing.....");
			LOG.info("=========================================================================================");
			for(String key : registHandle.keySet()){
				String value = registHandle.get(key);
				LOG.info("key name :" + key + " class name: " + value);
				propertyHandlerMapping.addHandler(key,  Class.forName(value));
			}
			LOG.info("=========================================================================================");
			rpcServer.setHandlerMapping(propertyHandlerMapping);
			rpcServer.setMaxThreads(50);
			XmlRpcServerConfigImpl xmlRpcServerConfigImpl = (XmlRpcServerConfigImpl) rpcServer.getConfig();
			xmlRpcServerConfigImpl.setEnabledForExceptions(true);
			xmlRpcServerConfigImpl.setContentLengthOptional(false);
			webServer.start();
			LOG.info("RPC Service has started !!!!!!!........");
		} catch (IOException e) {
			LOG.error("IOException ", e);
		} catch (XmlRpcException e) {
		    LOG.error("XmlRpcException ", e);
		} catch (ClassNotFoundException e) {
		    LOG.error("ClassNotFoundException ", e);
		}
	}
	
	public void run() {
		startService();
	}
	
	public void stopService(){
		LOG.info("Stoping RPC Service please wait !!!....");
		this.webServer.shutdown();
		LOG.info("RPC Service has down !!!!!!!........");
	}
	
	public RPCServiceThread(int port,Map<String, String> registHandle) {
		this.port = port;
		this.registHandle = registHandle;
	}
}
