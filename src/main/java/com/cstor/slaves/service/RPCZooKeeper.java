package com.cstor.slaves.service;

import java.io.IOException;

import com.cstor.utils.JKWatcher;
import com.cstor.utils.Properties;

public class RPCZooKeeper {
	
	private static JKWatcher zooKeeper = null;
	public synchronized static void init(){
		if(zooKeeper == null){
			try {
				zooKeeper = new JKWatcher(Properties.getInstance());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	public static JKWatcher getZooKeeper(){
		return zooKeeper;
	}
}
