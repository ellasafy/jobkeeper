package com.cstor.jobkeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JKWatcher implements Watcher{
	private static final Logger LOG = LoggerFactory.getLogger(JKWatcher.class);
	
	public void process(WatchedEvent event) {
		//get connection state
				KeeperState state = event.getState();
				//get event type
				EventType eventType = event.getType();
				//get event path
				String eventPath = event.getPath();
				if (KeeperState.SyncConnected == state) {
					if (EventType.None == eventType) {
						LOG.info("Catch Watcher Event None");
					}
					if (EventType.NodeCreated == eventType) {
						LOG.info("Catch Watcher Event NodeCreated, path" + eventPath);
					}
					if (EventType.NodeDataChanged == eventType) {
						LOG.info("Catch Watcher Event NodeDataChanged, path" + eventPath);
					} 
					if (EventType.NodeDeleted == eventType) {
						LOG.info("Catch Watcher Event NodeDeleted, path" + eventPath);
					}
				} else if (KeeperState.Disconnected == state) {
					LOG.info("disconnect from server");
				} else if (KeeperState.AuthFailed == state) {
					LOG.info("authFailed from server");
				} else if (KeeperState.Expired == state) {
					LOG.info("the connection expired");
				}
				
	}

}
