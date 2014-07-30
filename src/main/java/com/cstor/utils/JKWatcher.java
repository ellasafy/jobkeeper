package com.cstor.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cstor.master.event.EventHandler;


public class JKWatcher implements Watcher {
	private static final Logger LOG = LoggerFactory
			.getLogger(JKWatcher.class);
	private String connectString;
	private JKZooKeeper jkZooKeeper;
	private EventHandler handler = new EventHandler();
	public CountDownLatch saslLatch = new CountDownLatch(1);
	public static final ArrayList<ACL> ACL = Ids.OPEN_ACL_UNSAFE;

	public JKWatcher(Properties properties) throws IOException {
		this(properties.getConnectString(), properties.getSessionTimeout(), properties.getConnectionRetryCount(), 1000);
	}

	public JKWatcher(String connectString, int seesionTimeout, int maxRetries,int retryIntervalMillis) throws IOException {
		this.connectString = connectString;
		this.jkZooKeeper = new JKZooKeeper(connectString,seesionTimeout, this, maxRetries, retryIntervalMillis);
	}

	public JKWatcher(String quorum) throws IOException {
		this(quorum, 180 * 1000, 3, 1000);
	}

	public String prefix(final String str) {
		return this.toString() + " " + str;
	}
	public JKZooKeeper getJKZooKeeper() {
		return jkZooKeeper;
	}

	public String getConnectString() {
		return connectString;
	}
	public void process(WatchedEvent event) {
		LOG.debug("Received watcher Event", event);
	
		if (event != null) {
			// None, NodeCreated, NodeDeleted, NodeDataChanged, NodeChildrenChanged;
			EventType eventType = event.getType();
			// Disconnected,NoSyncConnected,SyncConnected,AuthFailed,ConnectedReadOnly,SaslAuthenticated,Expired
//			KeeperState state = event.getState();
//			
			String path = event.getPath();
			switch (eventType) {
		    case None :
			    break;
		    case NodeCreated :
		    	LOG.debug("node created ", event.toString());
		    	NodeCreated(path);
		    	break;
		    case NodeDeleted :
		    	LOG.debug("node Deleted ", event.toString());
		    	break;
		    case NodeDataChanged :
		    	LOG.debug("node DataChanged ", event.toString());
		    	NodeDataChanged(path);
		    	break;
		    case NodeChildrenChanged :
		    	LOG.debug("node ChildrenChanged ", event.toString());
		    	break;
		}
		}
		
	}
	
	private void NodeCreated(String path) {
		if (JKZNodeInfo.ZNODE_JOBS == path.substring(0, path.lastIndexOf("/"))) {
    		handler.nodeCreatedEvent(path);
    	}
	}

	private void NodeDataChanged(String path) {
		if (JKZNodeInfo.ZNODE_JOBS == path.substring(0, path.lastIndexOf("/"))) {
			handler.dataChangeEvent(path);
		}
	}
	

	private void connectionEvent(WatchedEvent event) {
		System.out.println(event.getState());
		switch (event.getState()) {
			case SyncConnected :
				long finished = 2000;
				while (System.currentTimeMillis() < finished) {
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					if (this.jkZooKeeper != null)
						break;
				}
				if (this.jkZooKeeper == null) {
					LOG.error("ZK is null on connection event -- see stack trace for the stack trace when constructor was called on this zkw");
					throw new NullPointerException("ZK is null");
				}
				break;
			case SaslAuthenticated :
				saslLatch.countDown();
				break;

			case AuthFailed :
				saslLatch.countDown();
				break;
			case Disconnected :
				LOG.debug(prefix("Received Disconnected from ZooKeeper, ignoring"));
				break;

			case Expired :
				saslLatch.countDown();
				break;
		}
	}

	public void sync(String path) {
		this.jkZooKeeper.sync(path, null, null);
	}
	public void keeperException(KeeperException ke) throws KeeperException {
		LOG.error(prefix("Received unexpected KeeperException, re-throwing exception"),ke);
		throw ke;
	}
	public void interruptedException(InterruptedException ie) {
		LOG.debug(prefix("Received InterruptedException, doing nothing here"),ie);
		Thread.currentThread().interrupt();
	}
	public void close() {
		try {
			if (jkZooKeeper != null) {
				jkZooKeeper.close();
			}
		} catch (InterruptedException e) {
		}
	}
}
