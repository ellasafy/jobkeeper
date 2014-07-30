package com.cstor.slaves.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cstor.JKWatcher;
import com.cstor.lock.service.LockCallback;
import com.cstor.lock.service.LockService;
import com.cstor.slaves.bean.Message;
import com.cstor.slaves.bean.TaskNode;
import com.cstor.userInterface.Job;
import com.cstor.utils.JKZNodeInfo;
import com.cstor.utils.Properties;
import com.cstor.utils.SerializableUtil;

public class TaskNodeBasicService {

	private static final Logger LOG = LoggerFactory.getLogger(TaskNodeBasicService.class);
	
	public boolean regist(byte[] dataBuffer){
		LOG.debug("Method regist in");
		Object object = SerializableUtil.readObject(dataBuffer);
		if(object == null){
			LOG.error("Give tasknode info is null, please check!!!! This error is very important !!!! ");
			return false;
		}else{
			LOG.debug("object is not null");
			TaskNode taskNode = (TaskNode) object;
			final String hostName = taskNode.getHostName();
			final String taskNodeZKName = JKZNodeInfo.ZNODE_SLAVES + "/" +hostName;
			final byte[] data = dataBuffer;
			final LockService lockService = new LockService();
			boolean result = (Boolean) lockService.doInLock(hostName, new LockCallback() {
				public Object callback(JKWatcher jkWatcher) {
					try {
						if(!jkWatcher.getJobkeeper().exists(taskNodeZKName, false)){
							jkWatcher.getJobkeeper().create(taskNodeZKName, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						}
						return true;
					} catch (KeeperException e) {
						LOG.error("KeeperException error", e);
					} catch (InterruptedException e) {
						LOG.error("InterruptedException error", e);
					}
					return false;
				}
			});
			return result;
		}
	}
	
	/**
	 * 获取当前任务
	 * @param hostName ip地址
	 * @return 
	 */
	public List<String> getCurrentJobList(String hostName){
		LOG.debug("Method getCurrentJobList in");
		String taskNodeZKName = JKZNodeInfo.ZNODE_SLAVES + "/" + hostName;
		JKWatcher jkWatcher = null;
		try {
			jkWatcher = new JKWatcher(Properties.getInstance());
			byte[] data = jkWatcher.getJobkeeper().getData(taskNodeZKName, false, null);
			if(data == null){
				LOG.debug("get data null, the name is " + taskNodeZKName);
				return null;
			}else{
				List<String> jobZkNameList = null;
				LOG.debug("get data successfully, the name is " + taskNodeZKName);
				TaskNode taskNode = (TaskNode) SerializableUtil.readObject(data);
				if (taskNode == null) {
					LOG.error("taskNode is null");
				} else {
					jobZkNameList = taskNode.getJobZkNameList();
				}
				return jobZkNameList;
			}
		} catch (KeeperException e) {
			if (e.code() == Code.NONODE) {
				LOG.error("Method: getCurrentJobList, get joblist of " + hostName + " error, node does not exists" + e); 
			}
			LOG.error("Method: getCurrentJobList, get joblist of " + hostName + "KeeperException error " + e);
		} catch (InterruptedException e) {
			LOG.error("Method: getCurrentJobList, get joblist of " + hostName + "InterruptedException error " + e);
		} catch (Exception e) {
			LOG.error("error ", e);
		}
		finally{
			if (jkWatcher != null) {
				jkWatcher.close();
			}
		}
		LOG.debug("Method getCurrentJobList out");
		return null;
	}
	
	public String getJobInfoByHost(String hostName) {
		String taskNodeZKName = JKZNodeInfo.ZNODE_SLAVES + "/" + hostName;
		JKWatcher jkWatcher = null;
		Map<String, Integer> jobInfos = new HashMap<String, Integer>();
		JSONObject object = new JSONObject();
		try {
			jkWatcher = new JKWatcher(Properties.getInstance());
			byte[] data = jkWatcher.getJobkeeper().getData(taskNodeZKName, false, null);
			if(data == null){
			}else{
				TaskNode taskNode = (TaskNode) SerializableUtil.readObject(data);
				List<String> jobZkNameList = taskNode.getJobZkNameList();
				for (String jobZkName : jobZkNameList) {
					byte[] jobData = jkWatcher.getJobkeeper().getData(jobZkName, false, null);
					if (jobData == null) {
						jobInfos.put(jobZkName, null);
					} else {
						Job job = (Job) SerializableUtil.readObject(jobData);
						jobInfos.put(jobZkName, job.getStatus());
					}
					
				}
				
			}
			object.put("returnCode", Message.SUCCESS);
			object.put("map", jobInfos);
			return object.toString();
		} catch (KeeperException e) {
			if (e.code() == Code.NONODE) {
				LOG.error("Method: getCurrentJobList, get joblist of " + hostName + " error, node does not exists" + e); 
			}
			LOG.error("Method: getCurrentJobList, get joblist of " + hostName + "KeeperException error " + e);
		} catch (InterruptedException e) {
			LOG.error("Method: getCurrentJobList, get joblist of " + hostName + "InterruptedException error " + e);
		}finally{
			if (jkWatcher != null) {
				jkWatcher.close();
			}
		}
		object.put("returnCode", Message.TASKNODEERROR);
		return object.toString();
	}
	
	/**
	 * 更加jobname获取job对象
	 * @param jobZKName
	 * @return
	 */
	public synchronized Job getJobFromZK(String jobZKName) throws KeeperException {
		LOG.debug("Method in : getJobFromZK ", "jobZKName : " +  jobZKName);
		JKWatcher jkWatcher = null;
		Job job = null;
		try {
			jkWatcher = new JKWatcher(Properties.getInstance());
			byte[] dataBuffer = jkWatcher.getJobkeeper().getData(jobZKName, true, null);
			job = (Job) SerializableUtil.readObject(dataBuffer);
		} catch (KeeperException e) {
		    throw e;
		} catch (InterruptedException e) {
			LOG.error("Run into an InterruptedException", e);
		} finally {
			if (jkWatcher != null) {
				jkWatcher.close();
			}
		}
		LOG.debug("Method out : getJobFromZK ", "jobZKName : " +  jobZKName);
		return job;
	}	
	
	public boolean cleanSuccessJobInfo(final String jobCurrentName,final String hostName){
		LockService lockService = new LockService();
		boolean result = (Boolean)lockService.doInLock(hostName, new LockCallback() {
			public Object callback(JKWatcher jkWatcher) {
				// TODO Auto-generated method stub
				String taskNodeZkName = JKZNodeInfo.ZNODE_SLAVES + "/" + hostName;
				try {
					byte[] dataBuffer = jkWatcher.getJobkeeper().getData(taskNodeZkName, false, null);
					if(dataBuffer == null){
						return false;
					}else{
						TaskNode taskNode = (TaskNode) SerializableUtil.readObject(dataBuffer);
//						String jobCurrentName  = JKZNodeInfo.ZNODE_JOBS + "/" +jobName;
						taskNode.getJobZkNameList().remove(jobCurrentName);
						dataBuffer = SerializableUtil.writeObject(taskNode);
						System.out.println(taskNodeZkName);
						jkWatcher.getJobkeeper().setData(taskNodeZkName, dataBuffer, -1);
						return true;
					}
				} catch (KeeperException e) {
					LOG.error("KeeperException", e);
				} catch (InterruptedException e) {
					LOG.error("InterruptedException", e);
				}
				return false;
			}
		});
		return result;
	}
	
	public boolean logoff(byte[] dataBuffer){
		return false;
	}
	
}
