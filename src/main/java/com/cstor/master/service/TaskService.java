package com.cstor.master.service;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cstor.JKWatcher;
import com.cstor.lock.service.LockCallback;
import com.cstor.lock.service.LockService;
import com.cstor.slaves.service.TaskNodeBasicService;
import com.cstor.userInterface.Job;
import com.cstor.userInterface.JobRunningClass;
import com.cstor.userInterface.Status;
import com.cstor.utils.JKZNodeInfo;
import com.cstor.utils.SerializableUtil;

public class TaskService {
	private static final Logger LOG = LoggerFactory.getLogger(TaskService.class);
	private TaskNodeBasicService taskNodeBasicService = new TaskNodeBasicService();
	private Lock _Lock = new ReentrantLock();
	/**
	 * 启动任务，启动时检查状态，如果未启动，启动任务
	 * @param jobRunningClass
	 */
	public  void startJob(final JobRunningClass jobRunningClass) {
		LOG.debug("Method startJob in" );
		final String jobName = jobRunningClass.getJob().getName();
		LOG.debug("The jobName is :" + jobName);
		_Lock.lock();
		try {
		    LockService lockService = new LockService();
            boolean result = (Boolean) lockService.doInLock(jobName, new LockCallback() {
                public Object callback(JKWatcher jkWatcher) {
                    Job job = null;
                    try {
                        job = taskNodeBasicService.getJobFromZK(JKZNodeInfo.ZNODE_JOBS + "/" + jobName);
                    } catch (KeeperException e) {
                       
                    }
                    
                    if (job != null) {
                        LOG.debug("job : ", job);
                        int jobStatus = job.getStatus();
                        if (jobStatus == Status.INIT) {
                            long startTime = jobRunningClass.getJob().getStartTime();
                            long currentTime = System.currentTimeMillis();
                            LOG.debug("startTime : " + startTime + " currentTime : " + currentTime);
                                jobRunningClass.getJob().setStatus(Status.RUNNING);
                                jobRunningClass.start();
                                
                                String jkZkName = JKZNodeInfo.ZNODE_JOBS + "/" + jobName;
                                try {
                                    byte[] data = SerializableUtil.writeObject(jobRunningClass.getJob());
                                    jkWatcher.getJobkeeper().setData(jkZkName, data, -1);
                                    return true;
                                } catch (Exception e) {
                                    LOG.error("set data error, jkname " + jkZkName, e);
                                    return false;
                                } 
                        } else {
                            LOG.info("job status is " + job.getStatus() + " is not init");
                            return true;
                        }
                    } else {
                        LOG.warn("job is null");
                        return false;
                    }
                }
            });
			LOG.info("startJob end, the result " + result);
		} finally {
			_Lock.unlock();
		}
		
		LOG.debug("Method startJob out ..." );
	}
	
	/**
	 * 停止任务，并把任务设置为待删除状态
	 * @param jobRunningClass
	 */
	public  void stopJob(final JobRunningClass jobRunningClass) {
		LOG.debug("Method stopJob in..");
		final String jobName = jobRunningClass.getJob().getName();
		_Lock.lock();
		try {
		    LockService lockService = new LockService();
	        boolean result = (Boolean) lockService.doInLock(jobName, new LockCallback() {
	            public Object callback(JKWatcher jkWatcher) {
	                Job job = null;
                    try {
                        job = taskNodeBasicService.getJobFromZK(JKZNodeInfo.ZNODE_JOBS + "/" + jobName);
                    } catch (KeeperException e) {
                       
                    }
	                LOG.debug("job : ", job);
	                if (job != null) {
	                    LOG.debug("start to stop the job " + jobName);
	                    int jobStatus = job.getStatus();
	                    LOG.debug("stopJob Method : the job status is : " + jobStatus);
	                    //任务为停止状态
	                    if (jobStatus == Status.STOP) {
	                        jobRunningClass.end();
	                        LOG.info("stop job end, the status is : " + jobRunningClass.getJob().getStatus());
	                        jobRunningClass.getJob().setStatus(Status.STOPPED);
	                        
	                        
	                        String jkZkName = JKZNodeInfo.ZNODE_JOBS + "/" + jobName;
	                        try {
	                            byte[] data = SerializableUtil.writeObject(jobRunningClass.getJob());
	                            jkWatcher.getJobkeeper().setData(jkZkName, data, -1);
	                            return true;
	                        } catch (Exception e) {
	                            LOG.error("set data error, jkname " + jkZkName, e);
	                        }
	                        return false;
	                    } 
	                    LOG.info("job status is " + job.getStatus() + " is not stop");
	                    return false;
	                } else {
	                    return false;
	                }
	            }
	        });
			LOG.info("stopJob sync result " + result);
		} finally {
			_Lock.unlock();
		}
		
		LOG.debug("Method stopJob out");
	}
	
	/**
	 * 同步任务状态
	 * @param jobRunningClass
	 */
	public synchronized void syncJob(JobRunningClass jobRunningClass) {
		if (true) {
			while (jobRunningClass.sync()) {
				LOG.info("------->stopped job done " + jobRunningClass.getJob().getName());
				break;
			}
		}
	}

}
