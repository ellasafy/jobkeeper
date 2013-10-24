package com.cstor;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JkWatcher implements Watcher {
	private static final Logger LOG = LoggerFactory.getLogger(JkWatcher.class);
	private ZooKeeper zk = null;
	AtomicInteger seq = new AtomicInteger();
	private CountDownLatch connectedSemaphore = new CountDownLatch(1);
	private static final String LOG_PREFIX_OF_MAIN = "【Main】";
	
	public void buildConnection(String ips, int timeout) {
		try {
			LOG.info(LOG_PREFIX_OF_MAIN + "start to build new connection...");
			zk = new ZooKeeper(ips, timeout, this);
			LOG.info(LOG_PREFIX_OF_MAIN + "build successfully");
			try {
				connectedSemaphore.await();
			} catch(InterruptedException e) {
				LOG.error("connection thread wait is interrupted");
			}
			
		} catch (IOException e) {
			LOG.error("create ssetion run into errors : " + e);
		}
	}
	
	
	public void releaseConnection() {
		if (null != this.zk) {
			try {
				this.zk.close();
			} catch (InterruptedException e) {
				LOG.error(LOG_PREFIX_OF_MAIN + "release connection comes to error: " + e);
			}
			
		}
	}
	

	public void process(WatchedEvent event) {
		//get connection state
		KeeperState state = event.getState();
		//get event type
		EventType eventType = event.getType();
		//get event path
		String eventPath = event.getPath();
		String logPrefix = "【Watcher-" + this.seq.incrementAndGet() + "】";
		if (KeeperState.SyncConnected == state) {
			if (EventType.None == eventType) {
				LOG.info(logPrefix + "node is live, and the path is: " + eventPath);
				connectedSemaphore.countDown();
			}
			if (EventType.NodeCreated == eventType) {
				LOG.info(logPrefix + "create node action, path : " + eventPath);
				Stat st = this.exits(eventPath, this);
				if (null != st) {
					LOG.info("eventPath " + eventPath + "exits " + st.toString());
				}
			}
			if (EventType.NodeDataChanged == eventType) {
				LOG.info(logPrefix + "date changed action, path : " + eventPath);
			} 
			if (EventType.NodeDeleted == eventType) {
				LOG.info(logPrefix + "delete node action, path : " + eventPath);
			}
		} else if (KeeperState.Disconnected == state) {
			LOG.info("disconnect from server");
		} else if (KeeperState.AuthFailed == state) {
			LOG.info("authFailed from server");
		} else if (KeeperState.Expired == state) {
			LOG.info("the connection expired");
		}
		
	}
	
	public void deleteNode(String path) {
		try {
//			Stat state = this.exits(path, this);
//			if (null != state) {
				this.zk.delete(path, -1);
//			}
			LOG.info(LOG_PREFIX_OF_MAIN + "successfully delete " + path);
		} catch (InterruptedException e) {
			LOG.error("comes to a InterruptedException :" + e);
		} catch (KeeperException e) {
			LOG.error("comes to a KeeperException :" + e);
		}
		
	}
	
	public void createNode(String path, String data) {
		try {
//			Stat state = this.zk.exists(path, true);
//			LOG.info(path + "exists");
			if (true) {
				String result = this.zk.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				LOG.info(LOG_PREFIX_OF_MAIN + "create " + path + "successfully");
			}
		} catch (Exception e) {
			LOG.error("create Node comes to an error");
		}
	}
	public byte[] getData(String path) {
		byte[] data = null;
		try {
			data = this.zk.getData(path,true, null);
			
		} catch(Exception e) {
			LOG.error("getData error + " + path, e);
		}
		return data;
	}
	
	public void setData(String path, Object obj) {
		try {
			this.zk.setData(path, (byte[])obj, -1);
		} catch (Exception e) {
			LOG.error("setData error + " + path, e);
		}
	}
	
	public void testConnection() {
		States state = this.zk.getState();
		if (state.isAlive()) {
			LOG.info(LOG_PREFIX_OF_MAIN + "the ssetion is alive");
		}
	}
	
	public Stat exits(String path, Watcher watcher) {
		try {
			Stat state = this.zk.exists(path, this);
			return state;
		} catch (KeeperException e) {
			LOG.error("KeeperException error in check the exist node : " + path + " : " + e);
		} catch (InterruptedException e) {
			LOG.error("InterruptedException error in check the exist node : " + path + " : " + e);
		}
		return null;
	}

}
