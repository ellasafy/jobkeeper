package com.cstor.jobkeeper;

import java.io.IOException;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;


public class JobKeeper {
	private ZooKeeper zk = null;
	
	public JobKeeper(String connectString, int sessionTimeout, Watcher watcher) throws IOException {
		connection(connectString, sessionTimeout, watcher);
	   }
	
	public void connection(String connectString, int sessionTimeout, Watcher watcher) throws IOException {
		zk = new ZooKeeper(connectString, sessionTimeout, watcher);
	}

}
