package com.cstor;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cstor.jobkeeper.JobKeeper;

public class JkWatcherMain {
	private static final Logger LOG = LoggerFactory.getLogger(JkWatcherMain.class);
	public static void main(String[] args) {
		String ip = "192.168.254.128";
//		PropertyConfigurator.configure("src/main/resources/log4j.properties");
		System.setProperty("log4j.configuration", "log4j.properties");
      
		JkWatcher watcher = new JkWatcher();
		JobKeeper jk = new JobKeeper();
		jk.connection(ip, 30000, watcher);
		try {
			TimeUnit.SECONDS.sleep(3);
		} catch(Exception e) {
			
		}
		
		LOG.info("end");
	}

}
