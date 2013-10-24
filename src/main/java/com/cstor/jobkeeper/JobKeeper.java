package com.cstor.jobkeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cstor.jobkeeper.exception.JkException;


public class JobKeeper {
	private static final Logger LOG = LoggerFactory.getLogger(JobKeeper.class);
	
	private ZooKeeper zk = null;
	
	private Lock _zookeeperLock = new ReentrantLock();
	
//	private static final int MAX_TIME = 3000;
	
	public JobKeeper() {};
	
	public JobKeeper(String connectString, int sessionTimeout, Watcher watcher) 
			throws IllegalStateException, JkException{
		this.connection(connectString, sessionTimeout, watcher);
	}
	
	/**
	 * build a new connection to the server
	 * @param connectString
	 * @param sessionTimeout
	 * @param watcher
	 * @throws IOException
	 */
	public void connection(String connectString, int sessionTimeout, Watcher watcher) {
		LOG.debug("Begin to connect servers...");
		_zookeeperLock.lock();
		try {
			if (zk != null) {
				 throw new IllegalStateException("zk client has already been started");
			} else {
				try {
					zk = new ZooKeeper(connectString, sessionTimeout, watcher);
				} catch(IOException e) {
					throw new JkException("create connection to servers failed", e);
				}
			}
		} finally {
			_zookeeperLock.unlock();
		}
		LOG.debug("End connect to servers ");
	}
	/**
	 * close the connection to the server
	 * @throws InterruptedException
	 */
	public void close() {
		_zookeeperLock.lock();
		try {
			if (null != zk) {
				try {
					zk.close();
					zk = null;
				} catch(InterruptedException e) {
					throw new JkException("close connection failed", e);
				}
				
			}
		} finally {
			_zookeeperLock.unlock();
		}
		
	}
	
	public String createPersistent(String path, byte[] data) 
			throws KeeperException, InterruptedException {
		return zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}
    
	public String createEphemeral(String path, byte[] data)
			throws KeeperException, InterruptedException {
		return zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}
	
	public String create(String path, byte[] data, CreateMode mode)
			throws KeeperException, InterruptedException {
		return zk.create(path, data, Ids.OPEN_ACL_UNSAFE, mode);
	}

	public void delete(String path) throws InterruptedException,
			KeeperException {
		zk.delete(path, -1);
	}

	public boolean exists(String path, boolean watch) throws KeeperException,
			InterruptedException {
		return zk.exists(path, watch) != null;
	}

	public List<String> getChildren(final String path, final boolean watch)
			throws KeeperException, InterruptedException {
		return zk.getChildren(path, watch);
	}

	public byte[] readData(String path, Stat stat, boolean watch)
			throws KeeperException, InterruptedException {
		return zk.getData(path, watch, stat);
	}

	public void writeData(String path, byte[] data) throws KeeperException,
			InterruptedException {
		writeData(path, data, -1);
	}

	public void writeData(String path, byte[] data, int version)
			throws KeeperException, InterruptedException {
		zk.setData(path, data, version);
	}

	public Stat writeDataReturnStat(String path, byte[] data, int version)
			throws KeeperException, InterruptedException {
		return zk.setData(path, data, version);
	}

}
