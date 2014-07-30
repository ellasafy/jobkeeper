package com.cstor.master.service;

import java.util.ArrayList;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cstor.JKWatcher;
import com.cstor.lock.service.LockCallback;
import com.cstor.lock.service.LockService;
import com.cstor.slaves.bean.TaskNode;
import com.cstor.userInterface.Job;
import com.cstor.userInterface.Status;
import com.cstor.utils.JKZNodeInfo;
import com.cstor.utils.Properties;
import com.cstor.utils.SerializableUtil;

public class MasterSyncThread implements Runnable{
	private static final Logger LOG = LoggerFactory.getLogger(MasterSyncThread.class);
	private JKWatcher jkWatcher;
	
	/**
	 * 查看master节点是否存在，不存在则新建节点，判断是否是master节点
	 * master节点会删掉tasknode，并更改任务的id为exception
	 */
	private void service(){
		String masterZkName = JKZNodeInfo.ZNODE_MASTER + "/" + Properties.getInstance().getHostName();
		try {
			if(!jkWatcher.getJobkeeper().exists(masterZkName, false)){
				jkWatcher.getJobkeeper().create(masterZkName, Properties.getInstance().getHostName().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			}
//			if(isLeader()){
//				LOG.info("This is a master node");
//				List<String> deadTaskNodes = findOutDeadTaskNode();
//				for (String ip : deadTaskNodes) {
//					procDeadTaskNode(ip);
//				}
//			}
		} catch (KeeperException e) {
			if (e.code() == Code.SESSIONEXPIRED) {
				LOG.warn("Get the session expired exception, now to restart the session.. ");
				if (jkWatcher != null) {
					LOG.debug("The session is not null, try to close");
					jkWatcher.close();
				} 
				LOG.debug("Start a new ssesion...");
				jkWatcher = new JKWatcher(Properties.getInstance());
			}
			LOG.error("KeeperException error", e);
		} catch (InterruptedException e) {
			LOG.error("InterruptedException error", e);
		}
	}
	
	/**
	 * 获得tasknode的ip
	 * @return
	 */
	private List<String> findOutDeadTaskNode(){
		LOG.debug("try to find dead tasknode..");
		List<String> deadTaskNodes = new ArrayList<String>();
		String deadTaskNodeName = null;
		try {
			List<String> onlineTaskNodes = jkWatcher.getJobkeeper().getChildren(JKZNodeInfo.ZNODE_MASTER, false);
			List<String> currentTaskNodes = jkWatcher.getJobkeeper().getChildren(JKZNodeInfo.ZNODE_SLAVES, false);
			if(onlineTaskNodes.size() < currentTaskNodes.size()){
				for(String curentTaskNode : currentTaskNodes){
					if(!onlineTaskNodes.contains(curentTaskNode)){
						LOG.info("Find dead slave ........ZK name is :" + deadTaskNodeName);
						deadTaskNodes.add(curentTaskNode);
					}
				}
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return deadTaskNodes;
	}
	
	private void procDeadTaskNode(final String hostName){
		LockService lockService = new LockService();
		final String jkTaskNodeName = JKZNodeInfo.ZNODE_SLAVES + "/" + hostName;
		lockService.doInLock(hostName, new LockCallback() {
			public Object callback(JKWatcher jkWatcher) {
				try {
					byte[] dataBuffer = jkWatcher.getJobkeeper().getData(jkTaskNodeName, false, null);
					TaskNode taskNode = (TaskNode) SerializableUtil.readObject(dataBuffer);
					if(taskNode == null){
						return false;
					}else{
						List<String> jobZkNameList = taskNode.getJobZkNameList();
						if(jobZkNameList.size() == 0){
							jkWatcher.getJobkeeper().delete(jkTaskNodeName, -1);
							return true;
						}
						int count = jobZkNameList.size();
						for(final String jobZkName : jobZkNameList){
							
								boolean flag = false;
								byte[] data = null;
								try {
									if (!jkWatcher.getJobkeeper().exists(jobZkName, false)) {
										data = null;
									} else {
										data = jkWatcher.getJobkeeper().getData(jobZkName, true, null);
									}
									if (data != null) {
										Job job = (Job) SerializableUtil.readObject(data);
										job.setStatus(Status.EXPECTION);
										data = SerializableUtil.writeObject(job);
										jkWatcher.getJobkeeper().setData(jobZkName, data, -1);
									}
								} catch (KeeperException e) {
									e.printStackTrace();
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
								if(flag){
									count--;
								}
							
						}
						if(count <= 0){
							jkWatcher.getJobkeeper().delete(jkTaskNodeName, -1);
						}
					}
					
				} catch (KeeperException e) {
					LOG.error("KeeperException error :" + e);
					e.printStackTrace();
				} catch (InterruptedException e) {
					LOG.error("InterruptedException error :" + e);
				}
				return false;
			}
		});
	}
	
	private boolean isLeader(){
		try {
			List<String> masterZkNames = jkWatcher.getJobkeeper().getChildren(JKZNodeInfo.ZNODE_MASTER, false);
			if(masterZkNames == null){
				return false;
			}else{
				String leader = masterZkNames.get(0);
				if(leader.contains(Properties.getInstance().getHostName())){
					return true;
				}else{
					return false;
				}
			}
		} catch (KeeperException e) {
			LOG.error("KeeperException error :" + e);
		} catch (InterruptedException e) {
			LOG.error("InterruptedException error :" + e);
		}
		return false;
	}
	
	public void run() {
		while(true){
			service();
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public MasterSyncThread() {
		this.jkWatcher = new JKWatcher(Properties.getInstance());
	}
}