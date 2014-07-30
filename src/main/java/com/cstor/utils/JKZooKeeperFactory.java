package com.cstor.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.zookeeper.Watcher;

public class JKZooKeeperFactory {
	
	private Properties properties;
	private static Map<JKZooKeeper, Boolean> clientPool;
	
	private void initJKZooKeeperPool(){
		JKZooKeeper jkZooKeeper = null;
		for(int i=0;i<properties.getClientPoolCount();i++){
			try {
				Watcher watcher = new JKWatcher(properties);
				jkZooKeeper = new JKZooKeeper(properties.getConnectString(), properties.getSessionTimeout()	, watcher, properties.getConnectionRetryCount(), 1000);
			} catch (IOException e) {
				e.printStackTrace();
				jkZooKeeper = null;
				continue;
			}
			if(jkZooKeeper != null) clientPool.put(jkZooKeeper, false);
		}
	}
	
	public static JKZooKeeper getConnection(){
		while(true){
			synchronized (JKZooKeeperFactory.clientPool) {
				for (JKZooKeeper jkZooKeeper : clientPool.keySet()) {
					if (!clientPool.get(jkZooKeeper)) {
						clientPool.put(jkZooKeeper, true);
						return jkZooKeeper;
					}
				}
			}
		}
	}
	
	private JKZooKeeperFactory(){
		this.properties = Properties.getInstance();
		JKZooKeeperFactory.clientPool = new HashMap<JKZooKeeper, Boolean>();
		initJKZooKeeperPool();
	}
	
	
}
