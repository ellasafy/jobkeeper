package com.cstor.master.service;

import java.lang.reflect.InvocationTargetException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cstor.slaves.service.TaskNodeBasicService;
import com.cstor.userInterface.Job;
import com.cstor.userInterface.JobRunningClass;
import com.cstor.userInterface.Status;
import com.cstor.utils.Properties;

public class JMasterProcess implements Runnable {
	private static final Logger LOG = LoggerFactory
			.getLogger(JMasterProcess.class);
	private List<String> deleteJobKeys;
	private TaskNodeBasicService taskNodeBasicService;
	private TaskService taskService;
	private Properties properties;

	public JMasterProcess(TaskNodeBasicService taskNodeBasicService,
			Properties properties) {
		this.taskNodeBasicService = taskNodeBasicService;
		this.properties = properties;
		
		this.deleteJobKeys = new LinkedList<String>();
		this.taskService = new TaskService();
	}

	public void run() {
        LOG.debug("start to running the process");
        String hostname = properties.getHostName();
		while (true) {
            try {
                List<String> jobZkNameList = taskNodeBasicService.getCurrentJobList(hostname);
                
                if (jobZkNameList == null) {
                    LOG.info("there is no job in task of " + hostname);
                } else {
                    LOG.info("=============System job list=============");
                    int index = 0;
                    LOG.info("There are " + jobZkNameList.size() + " jobs :");
                    for (String jobZkName : jobZkNameList) {
                        LOG.info("Job " + ++index + " : " + jobZkName);
                        Job job = null;
                        try {
                            job = taskNodeBasicService.getJobFromZK(jobZkName);
                        } catch(KeeperException e) {
                            if (e.code() == Code.NONODE) {
                                job = null;
                            } else {
                                continue;
                            }
                        }
                        
                        if (job != null) {
                            JobRunningClass jobRunningClass = getJobRunningClass(job);
                            LOG.info("Job Name : "
                                    + jobRunningClass.getJob().getName());
                            // 获取runningjob的job状态信息
                            int runningStatus = jobRunningClass.getJob()
                                    .getStatus();
                            LOG.info("Job Status : "
                                    + jobRunningClass.getJob().getStatus());
                            // job status 流程
                            procDifferentJobStatus(runningStatus,
                                    jobRunningClass);
                        } else {
                            deleteJobKeys.add(jobZkName);
                            LOG.warn("job " + jobZkName
                                    + " is null, cannot get from /jk/job/***");
                        }
                    }

                    deleteEndFinishJob();
                }

            } catch (Exception e) {
                LOG.error("get an error in while loop : " + e);
            }

            try {
                TimeUnit.MILLISECONDS.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

	}

	/**
	 * 更running status进行不同的处理
	 * @param runningStatus 当前运行的runningclass 的job状态
	 * @param jobRunningClass 当前运行的runningclass
	 */
	public void procDifferentJobStatus(int runningStatus,
			JobRunningClass jobRunningClass) {
		switch (runningStatus) {
		case Status.INIT:
			taskService.startJob(jobRunningClass);
			break;
		case Status.RUNNING:
			break;
		case Status.ERROR:
		case Status.FAIL:
		case Status.SUCCESS:
		case Status.EXPECTION:
//			while (true) {
//				if (jobRunningClass.sync()) {
//					break;
//				}
//			}
//			deleteJobKeys.add(jobRunningClass.getJob().getName());
			break;
		case Status.STOP:
			LOG.info("-------->start to stop job : " + jobRunningClass.getJob().getName());
			taskService.stopJob(jobRunningClass);
			break;
		case Status.STOPPED:
//			if (nowStatus == Status.STOP) {
//				LOG.info("nowStatus is 6, but running status is 7, now change nowstaus to 7");
//				taskService.syncJob(jobRunningClass);
//			}
			break;
		default:
			LOG.info("default -- " + runningStatus);
		}
	}

	
	/**
	 * 获取job对应的jobrunningclass
	 * @param jobZkName
	 * @return
	 */
	public JobRunningClass getJobRunningClass(String jobZkName) {
	    Job job = null;
	    try {
	        job = taskNodeBasicService.getJobFromZK(jobZkName);
	    } catch (KeeperException e) {
	        
	    }
		
	    if (job == null) {
	    	return null;
	    } 
	    return getJobRunningClass(job);
	}
	
	/**
	 * 更加job获取jobrunningclass
	 * @param job
	 * @return
	 */
	public JobRunningClass getJobRunningClass(Job job) {
		String jobProcClassName = job.getConfiguration().getJobProcClassName();
		try {
			JobRunningClass jobRunningClass = (JobRunningClass) Class
					.forName(jobProcClassName)
					.getConstructor(new Class[] { Job.class }).newInstance(job);
			return jobRunningClass;
		} catch (InstantiationException e) {
			LOG.error("InstantiationException error", e);
		} catch (IllegalAccessException e) {
			LOG.error("IllegalAccessException error", e);
		} catch (IllegalArgumentException e) {
			LOG.error("IllegalArgumentException error", e);
		} catch (InvocationTargetException e) {
			LOG.error("InvocationTargetException error", e);
		} catch (NoSuchMethodException e) {
			LOG.error("NoSuchMethodException error", e);
		} catch (SecurityException e) {
			LOG.error("SecurityException error", e);
		} catch (ClassNotFoundException e) {
			LOG.error("ClassNotFoundException error", e);
		}
		return null;
	}


	public void deleteEndFinishJob() {
		LOG.debug("Method deleteEndFinishJob in");
		for (String key : deleteJobKeys) {
//			String jobName = JKZNodeInfo.ZNODE_JOBS + "/" + key;
			taskNodeBasicService.cleanSuccessJobInfo(key, Properties
					.getInstance().getHostName());
		}
		deleteJobKeys.clear();
		LOG.debug("Method deleteEndFinishJob out");
	}

}
