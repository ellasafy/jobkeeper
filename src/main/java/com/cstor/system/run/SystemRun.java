package com.cstor.system.run;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.PropertyConfigurator;

import com.cstor.master.service.JMaster;

public class SystemRun {

	public static void main(String[] args) {
		System.setProperty("JOBKEEPER.CONF", args[0]);
		PropertyConfigurator.configure("../conf/log4j.properties");
		Map<String, String> registHandle = new HashMap<String, String>();
		registHandle.put("JOBSERVICECLASS", "com.cstor.slaves.service.JobService");
		registHandle.put("SERVERINFOCLASS", "com.cstor.master.service.ServerService");
		registHandle.put("TASKSERVICECLASS", "com.cstor.slaves.service.TaskNodeBasicService");
		registHandle.put("EVENTHANDLECLASS", "com.cstor.master.event.EventHandler");
		JMaster jMaster = new JMaster(6666, registHandle);
		jMaster.start();
	}
}
