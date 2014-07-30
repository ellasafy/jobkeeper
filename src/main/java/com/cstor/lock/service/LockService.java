package com.cstor.lock.service;

import java.util.Random;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cstor.JKWatcher;
import com.cstor.utils.Bytes;
import com.cstor.utils.JKZNodeInfo;
import com.cstor.utils.Properties;

public class LockService {
	private static final Logger LOG = LoggerFactory.getLogger(LockService.class);
	private JKWatcher jkWatcher;
    private Properties properties;
    
    public LockService (){
        this.properties = Properties.getInstance();
        String[] ips = this.properties.getConnectString().split(",");
        int index = 0;
        try {
            this.jkWatcher = new JKWatcher(properties);
        } catch (Exception e) {
            this.jkWatcher = null;
            LOG.error("get instance error " + e);
        }
        
    }
    /**
     * 锁方法
     * @param hostName 锁名称，一般为hostname
     * @param lockCallback callback方法，运行具体的实现
     * @return
     */
	public  Object doInLock(String hostName,LockCallback lockCallback){
		String lockZKName = JKZNodeInfo.ZNODE_LOCK + "/" + hostName;
		LOG.debug("Begin to get lock :" + lockZKName+"  .......");
		int count = 0;
		while(count++ < 20){
			LOG.debug("The " + count+"  time , try to get lock : " + lockZKName);
			if(getLock(lockZKName)){
				LOG.debug("Get lock success .... Lock name is :" + lockZKName);
				break;
			}
			sleep(new Random().nextInt(3) * 200l);
		}
		Object result = null;
		if (count < 10) {
			result = lockCallback.callback(jkWatcher);
		}
		realseLock();
		return result;
	}
	
	
	/**
	 * 获取锁，在lock下面建立节点
	 * @param lockZKName 锁名称
	 * @return
	 */
	private synchronized boolean getLock(String lockZKName) {
	    LOG.debug("start to get lock " + lockZKName);
		if(jkWatcher == null){
			LOG.warn("Can not init properties files please check it !!!!");
			return false;
		}
		String hostName = properties.getHostName();
		try {
			jkWatcher.getJobkeeper().create(lockZKName, Bytes.toBytes(hostName), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			return true;
		} catch (KeeperException e) {
		    if (e.code() == Code.NODEEXISTS) {
		        LOG.warn("try to lock " + lockZKName + " but it already exists, later try again");
		    } else {
		        LOG.warn("KeeperException while get lock ", e); 
		    }
		} catch (InterruptedException e) {
		    LOG.warn("InterruptedException ", e);
		}
		return false;
	}
	
	public void realseLock(){
	    LOG.debug("release the lock, close session");
	    if (jkWatcher != null) {
	        jkWatcher.close(); 
	        jkWatcher = null;
	        LOG.debug("close session successfully");
	    }
	}
	
	private void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			LOG.error("interrupted ", e);
		}
	}
}
