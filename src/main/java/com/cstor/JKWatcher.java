package com.cstor;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cstor.master.event.EventHandler;
import com.cstor.utils.JKZNodeInfo;
import com.cstor.utils.Properties;

public class JKWatcher implements Watcher{
	private static final Logger LOG = LoggerFactory.getLogger(JKWatcher.class);
	
	private JobKeeper jobkeeper = null;
	
	private EventHandler handle = new EventHandler();
	
	public JKWatcher() {
		this.setJobkeeper(new JobKeeper());
	}
	
	public JKWatcher(Properties properties) {
	    try {
	        this.setJobkeeper(new JobKeeper(properties, this));
	    } catch(Exception e) {
	        LOG.error("create session errer ", e);
	    }
		
	}
	
	public JKWatcher(String conn, int timeOut) {
	    try {
	        this.jobkeeper = new JobKeeper(conn, timeOut, this);
	    } catch (Exception e) {
	        LOG.error("create session errer ", e);
	    }
	}
	
	public void close() {
		if (jobkeeper != null) {
			jobkeeper.close();
			jobkeeper = null;
		} 
	}
	
	
	public void process(WatchedEvent event) {
		//get connection state
		KeeperState state = event.getState();
				//get event type
		EventType eventType = event.getType();
				//get event path
		String eventPath = event.getPath();
				//事件处理
		if (EventType.None == eventType) {
			LOG.debug("****Catch Watcher Event None");
		} else if (EventType.NodeCreated == eventType) {
			LOG.debug("*****Catch Watcher Event NodeCreated, path " + eventPath);
			nodeCreatedEvent(eventPath);
		}else if (EventType.NodeDataChanged == eventType) {
			LOG.debug("****Catch Watcher Event NodeDataChanged, path" + eventPath);
			nodeDataChangedEvent(eventPath);
		}else if (EventType.NodeDeleted == eventType) {
			LOG.debug("****Catch Watcher Event NodeDeleted, path" + eventPath);
		}
				//状态处理
		if (KeeperState.SyncConnected == state) {
			LOG.debug("SyncConnected to server");
		} else if (KeeperState.Disconnected == state) {
			LOG.debug("disconnect from server");
		} else if (KeeperState.AuthFailed == state) {
			LOG.debug("authFailed from server");
		} else if (KeeperState.Expired == state) {
			LOG.debug("the connection expired");
		}
				
	}
	
	/**
	 * 节点创建事件
	 * @param eventPath zk节点
	 */
	private void nodeCreatedEvent(String eventPath) {
	    String fatherNode = eventPath.substring(0, eventPath.lastIndexOf("/"));
	    if (JKZNodeInfo.ZNODE_JOBS.equals(fatherNode)) {
	        LOG.info("the node path is " + eventPath + " start to trigger");
	        handle.nodeCreatedEvent(eventPath.toString()); 
	    } else {
	        LOG.info("the node path is " + eventPath + " pass");
	    }
	   
	}
	
	/**
     * 节点数据变动事件
     * @param eventPath zk节点
     */
    private void nodeDataChangedEvent(String eventPath) {
        String fatherNode = eventPath.substring(0, eventPath.lastIndexOf("/"));
        if (JKZNodeInfo.ZNODE_JOBS.equals(fatherNode)) {
            LOG.info("the node path is " + eventPath + " start to trigger");
            handle.dataChangeEvent(eventPath);
        } else {
            LOG.info("the node path is " + eventPath + " pass");
        }
    }

	public JobKeeper getJobkeeper() {
		return jobkeeper;
	}

	public void setJobkeeper(JobKeeper jobkeeper) {
		this.jobkeeper = jobkeeper;
	}

}

