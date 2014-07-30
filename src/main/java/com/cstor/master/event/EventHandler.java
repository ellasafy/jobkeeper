package com.cstor.master.event;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cstor.master.service.TaskService;
import com.cstor.slaves.service.TaskNodeBasicService;
import com.cstor.userInterface.Job;
import com.cstor.userInterface.JobRunningClass;
import com.cstor.utils.Properties;

public class EventHandler {
	private static final Logger LOG = LoggerFactory.getLogger(EventHandler.class);
	private TaskNodeBasicService taskNodeBasicService = new TaskNodeBasicService();
	private TaskService taskService = new TaskService();
	
	
	public String nodeCreatedEvent(String path) {
		LOG.debug("Method nodeCreatedEvent in, path is : " + path);
		try {
			Job job = taskNodeBasicService.getJobFromZK(path);
			if (job == null) {
				LOG.warn("the job is null ");
			} else {
				String procHostName = job.getProcHostName();
				LOG.debug("the job procHost is : " + procHostName + " job : " + job.getName());
				String hostName = Properties.getInstance().getHostName();
				LOG.debug("the job procHost is : " + hostName + " job : " + job.getName());
				if (procHostName.equals(hostName)) {
					LOG.debug("local host, now run the local method");
					String jobProcClassName = job.getConfiguration()
							.getJobProcClassName();
					JobRunningClass jobRunningClass = (JobRunningClass) Class
							.forName(jobProcClassName)
							.getConstructor(new Class[] { Job.class })
							.newInstance(job);
					taskService.startJob(jobRunningClass);
				} else {
					LOG.debug("remote host, the host is " + procHostName + " now run the remote method");
					remoteEventHandle(procHostName, path, "EVENTHANDLECLASS.nodeCreatedEvent");
				}
			}
			return "success";
		
		} catch (Exception e) {
			LOG.error("comes to an error ", e);
		}
		LOG.debug("Method nodeCreatedEvent out");
		return "error";
	}
	/**
	 * 当状态改变的时候
	 * @param path
	 */
	public String dataChangeEvent(String path) {
	    Job job = null;
	    try {
	        job = taskNodeBasicService.getJobFromZK(path);
	    } catch (KeeperException e) {
	        
	    }
		if (job == null) {
			LOG.warn("the job is null ");
			return "error";
		}
		String procHostName = job.getProcHostName();
		String hostName = Properties.getInstance().getHostName();
		if (procHostName.equals(hostName)) {
			String jobProcClassName = job.getConfiguration()
					.getJobProcClassName();
			try {
				JobRunningClass jobRunningClass = (JobRunningClass) Class
						.forName(jobProcClassName)
						.getConstructor(new Class[] { Job.class })
						.newInstance(job);
				taskService.stopJob(jobRunningClass);
			} catch (Exception e) {
				LOG.error("comes to an error", e);
			}
			
		} else {
			remoteEventHandle(procHostName, path, "EVENTHANDLECLASS.dataChangeEvent");
		}
		return "success";
	}
	
	public String remoteEventHandle(String hostName, String jobZkName, String rpcMethod) {
	    LOG.debug("Start to send rpc to " + hostName);
		try {
			String url = "http://" + hostName + ":6666";
			XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
			config.setServerURL(new URL(url));
			XmlRpcClient client = new XmlRpcClient();
			List<Object> args = new ArrayList<Object>();
			args.add(jobZkName);
			client.setConfig(config);
			String result = (String)client.execute(rpcMethod,args);
			LOG.debug("Sending rpc successfully");
			return result;
		} catch (MalformedURLException e) {
			LOG.error("rpc exception ", e);
		} catch (XmlRpcException e) {
			LOG.error("rpc exception ", e);
		}
		return "error";
	}

}
