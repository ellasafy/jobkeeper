package com.cstor.userInterface;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cstor.JKWatcher;
import com.cstor.lock.service.LockCallback;
import com.cstor.lock.service.LockService;
import com.cstor.utils.JKZNodeInfo;
import com.cstor.utils.Properties;
import com.cstor.utils.SerializableUtil;

public abstract class JobRunningClass extends Thread {
	protected static final Logger LOG = LoggerFactory.getLogger(JobRunningClass.class);
	private Job job;

	public JobRunningClass(Job job) {
		this.job = job;
	}

	public Job getJob() {
		return job;
	}

	public void setJob(Job job) {
		this.job = job;
	}

//	public int getStatus() {
//		return job.getStatus();
//	}
//
//	public void setStatus(int status) {
//		job.setStatus(status);
//	}


	public abstract boolean begin();

	public abstract boolean end();

	public abstract boolean combine();

	public synchronized boolean sync() {
		final Job job = this.getJob();
		final String jobName = this.getJob().getName();
		LockService lockService = new LockService();
		boolean result = (Boolean) lockService.doInLock(jobName, new LockCallback() {
			public Object callback(JKWatcher jkWatcher) {
			    String jkZkName = JKZNodeInfo.ZNODE_JOBS + "/" + jobName;
			    try {
			        byte[] jobDate = jkWatcher.getJobkeeper().getData(jkZkName, false, null);
			        Job currrentJob = (Job)SerializableUtil.readObject(jobDate);
			        if (currrentJob == null) {
			            LOG.error("get " + jobName + " null");
			            return true;
			        }
			        if (currrentJob.getCreatedTime() == job.getCreatedTime()) {
			            byte[] data = SerializableUtil.writeObject(job);
			            jkWatcher.getJobkeeper().setData(JKZNodeInfo.ZNODE_JOBS + "/" + jobName, data, -1); 
			        }
			        else {
			            LOG.error("created time is not equal, the old job is changed");
			        }
				
					return true;
				} catch (KeeperException e) {
				    if (e.code() == Code.NONODE) {
				        LOG.warn("no node", e);
				        return true;
				    }
					LOG.error("KeeperException ", e);
				} catch (InterruptedException e) {
				    LOG.error("InterruptedException ", e);
				}
				return false;
			}
		});
		return result;
	}
	
	public void run() {
	    final String jobName = getJob().getName();
		LOG.info(jobName + " begin !!!");
		begin();
		//同步
		final int currentJobStatus = getJob().getStatus();
		LOG.info("running class over, the status is " + currentJobStatus);
	    LockService lockService = new LockService();
        lockService.doInLock(jobName, new LockCallback() {
            public Object callback(JKWatcher jkWatcher) {
                Job job = null;
                try {
                    byte[] data = jkWatcher.getJobkeeper().getData(JKZNodeInfo.ZNODE_JOBS + "/" 
                            + jobName, false, null);
                    job = (Job)SerializableUtil.readObject(data);
                } catch ( KeeperException  e) {
                    if (e.code() == Code.NONODE) {
                        LOG.warn("process over, get " + jobName + " error, cause it was deleted ");
                    } else {
                        LOG.error("process over, KeeperException while sync", e); 
                    }
                   return false;
                } catch (InterruptedException e) {
                    LOG.error("process over, InterruptedException while get " + jobName, e);
                    return false;
                }
               
                if (job != null) {
                    int jobStatus = job.getStatus();
                    LOG.info("get job from node, the status is " + jobStatus);
                    //只在启动失败的情况下更新状态，stop不更新
                    if (jobStatus == Status.INIT) {
                        job.setStatus(jobStatus);
                        String jkZkName = JKZNodeInfo.ZNODE_JOBS + "/" + jobName;
                        try {
                            byte[] data2 = SerializableUtil.writeObject(job);
                            jkWatcher.getJobkeeper().setData(jkZkName, data2, -1);
                            return true;
                        } catch (Exception e) {
                            LOG.error("set data error, jkname " + jkZkName, e);
                        }
                    } else {
                        LOG.warn("process over,job " + jobName + " already deleted");
                        return true;
                    }
                }
                return true;
            }
        });
	}
}
