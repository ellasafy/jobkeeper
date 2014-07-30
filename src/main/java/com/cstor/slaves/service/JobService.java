package com.cstor.slaves.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.sf.json.JSONObject;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cstor.JKWatcher;
import com.cstor.lock.service.LockCallback;
import com.cstor.lock.service.LockService;
import com.cstor.slaves.bean.Message;
import com.cstor.slaves.bean.TaskNode;
import com.cstor.userInterface.Job;
import com.cstor.userInterface.Status;
import com.cstor.utils.JKZNodeInfo;
import com.cstor.utils.Properties;
import com.cstor.utils.SerializableUtil;

public class JobService {

    private static final Logger LOG = LoggerFactory.getLogger(JobService.class);
    private static final int ZERO = 0;

    /**
     * 增加任务
     * 
     * @param dataBuffer
     *            任务流
     * @return
     */
    public  String addJob(byte[] dataBuffer) {
        LOG.debug("addJob Method in: ");
        final JSONObject object = new JSONObject();
        final Job job = (Job) SerializableUtil.readObject(dataBuffer);
        int ret = Message.JOBERROR;
        LOG.debug("job is comming-- > " + job);
        final String jobZKName = JKZNodeInfo.ZNODE_JOBS + "/" + job.getName();
        
        LockService lockService = new LockService();
        ret = (Integer) lockService.doInLock(job.getName(), new LockCallback() {
            public Object callback(JKWatcher jkWatcher) {
                List<String> masterNodes = new ArrayList<String>();
                try {
                    masterNodes = jkWatcher.getJobkeeper().getChildren(
                            JKZNodeInfo.ZNODE_MASTER, false);
                } catch (Exception e) {
                    LOG.error("get master error", e);
                    return Message.JOBERROR;
                }
               
                Collections.shuffle(masterNodes);
                if (masterNodes.isEmpty()) {
                    LOG.error("master nodes is empty");
                    return Message.JOBERROR;
                }
                final String procHostName = masterNodes.get(ZERO);
                if ((procHostName == null) || (procHostName.equals(""))) {
                    LOG.error("proc host is null");
                    return Message.JOBERROR;
                }
                object.put("procHostName", procHostName);
                //
                try {
                    byte[] data = null;
                    boolean flag = jkWatcher.getJobkeeper()
                            .exists(jobZKName, jkWatcher);
                    if (flag) {
                        data = jkWatcher.getJobkeeper()
                                .getData(jobZKName, true, null);
                    }
                    if (data == null) {
                        job.setProcHostName(procHostName);
                        data = SerializableUtil
                                .writeObject(job);
                        jkWatcher.getJobkeeper().exists(
                                jobZKName, true);
                        jkWatcher.getJobkeeper().create(
                                jobZKName, data,
                                Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT);
                        
                        LockService lockService = new LockService();
                        int ret = (Integer) lockService.doInLock(procHostName, new LockCallback() {
                            public Object callback(JKWatcher jkWatcher) {
                                try {
                                    String taskNodeZK = JKZNodeInfo.ZNODE_SLAVES + "/"
                                            + procHostName;
                                    byte[] data = jkWatcher.getJobkeeper().getData(taskNodeZK,
                                            true, null);
                                    TaskNode taskNode = (TaskNode) SerializableUtil
                                            .readObject(data);
                                    if (taskNode != null) {
                                        if (!taskNode.getJobZkNameList()
                                                .contains(jobZKName)) {
                                            taskNode.getJobZkNameList().add(jobZKName);
                                            data = SerializableUtil.writeObject(taskNode);
                                            jkWatcher.getJobkeeper().setData(taskNodeZK,
                                                    data, -1);
                                            LOG.info("add job " + job + " to task node "
                                                    + taskNode.getHostName()
                                                    + " successfully");
                                            return Message.JOBADDSUCCESS;
                                        } else {
                                            LOG.info("add jod error, because the task "
                                                    + taskNode.getHostName()
                                                    + " already exists the job " + job);
                                            return Message.TASKNODEJOBEXISTS;
                                        }
                                    }
                                    LOG.warn(taskNodeZK + " is null !!!!");
                                    return Message.JOBERROR;
                                
                                }catch (Exception e) {
                                    LOG.error("addJob Method: comes to an error when to add job to task", e);
                                    LockService lockService3 = new LockService();
                                    lockService3.doInLock(job.getName(), new LockCallback() {
                                        public Object callback(JKWatcher jkWatcher) {
                                            try {
                                                jkWatcher.getJobkeeper().delete(jobZKName); 
                                                return true;
                                            } catch (Exception e) {
                                                LOG.error("try to delete unseccussful job " + job.getName() + " error", e);
                                                return false;
                                            }
                                            
                                        }
                                    });
                                    return Message.JOBERROR;
                                }
                                
                            }
                        });
                        LOG.info("add proc is (" + procHostName + ")- > " + ret);
                        return ret;
                    } else {
                        LOG.info("This job " + jobZKName
                                + " is existed!!!");
                        return Message.JOBEXISTS;
                    }
                  
                } catch (KeeperException e) {
                    LOG.error("addJob keeperException ", e);
                    if (e.code() == Code.NONODE) {
                        LOG.error(
                                "Add task method error: when get job by JobName, node is note found",
                                e);
                        return Message.JOBNOTEXISTS;
                    }
                    return Message.JOBKEEPEREXCEPTION;
                } catch (InterruptedException e) {
                    LOG.error("addJob InterruptedException error:"
                            + e);
                    return Message.JOBINTERRUPTEDEXCEPTION;
                } catch (NullPointerException e) {
                    LOG.error("null point error:" + e);
                    return Message.JOBERROR;
                }

            }
        });
       
        LOG.debug("addJob end-- the result is " + ret);
        object.put("returnCode", ret);
        return object.toString();
    }
    
    /**
     * 停止运行中的job
     * 
     * @param jobName
     * @return
     */
    public int stopJob(final String jobName) {
        LOG.info("Method in stopJob " + jobName);
        LockService lockService = new LockService();
        Object result = lockService.doInLock(jobName, new LockCallback() {
            public Object callback(JKWatcher jkWatcher) {
                String jobZKName = JKZNodeInfo.ZNODE_JOBS + "/" + jobName;
                byte[] dataBuffer;
                try {
                    dataBuffer = jkWatcher.getJobkeeper().getData(jobZKName,
                            jkWatcher, null);
                    Job job = (Job) SerializableUtil.readObject(dataBuffer);
                    if (job == null) {
                        return Message.JOBNOTEXISTS;
                    }
                    LOG.debug("stopJob Method : the status is " + job.getStatus() + "start to stop");
                    if (job.getStatus() == Status.RUNNING) {
                        job.setStatus(Status.STOP);
                        dataBuffer = SerializableUtil.writeObject(job);
                        jkWatcher.getJobkeeper().setData(jobZKName, dataBuffer,
                                -1);
                        return Message.JOBSTOPSUCCESS;
                    } else {
                        return Status.STOPPED;
                    }
                } catch (KeeperException e) {
                    LOG.error("Stop job " + jobName + " KeeperException : " + e);
                    if (e.code() == Code.NONODE) {
                        return Message.JOBNOTEXISTS;
                    }
                } catch (InterruptedException e) {
                    LOG.error("Stop job " + jobName
                            + " InterruptedException : " + e);
                } catch (Exception e) {
                    LOG.error("Stop job " + jobName + " Exception : " + e);
                }
                return Message.JOBSTOPERROR;
            }
        });
        LOG.debug("stopJob Method end, the result is :" + result);
        return (Integer) result;
    }

    /**
     * 删除job信息
     * 
     * @param jobName
     * @return
     */
    public  boolean deleteJob(final String jobName) {
        LOG.debug("Method in deleteJob " + jobName);
        final String jobZKName = JKZNodeInfo.ZNODE_JOBS + "/" + jobName;
        
        LockService lockService = new LockService();
        Boolean result = (Boolean) lockService.doInLock(jobName, new LockCallback() {
            public Object callback(JKWatcher jkWatcher) {
                try {
                    byte[] data = jkWatcher.getJobkeeper().getData(jobZKName, false,
                            null);
                    Job job = (Job) SerializableUtil.readObject(data);
                    LOG.debug("job status is " + job.getStatus());
                    if (job.getStatus() != Status.INIT 
                            && job.getStatus() != Status.RUNNING
                            && job.getStatus() != Status.STOP) {
                        final String procName = job.getProcHostName();
                        LOG.debug("start to delete " + job.getName());
                        jkWatcher.getJobkeeper().delete(jobZKName, -1);
                        
                        LockService lockService = new LockService();
                        Boolean result = (Boolean) lockService.doInLock(procName, new LockCallback() {
                            public Object callback(JKWatcher jkWatcher) {
                                try {
                                    String taskNodeZK = "/jk/slaves/" + procName;
                                    byte[] tdata = jkWatcher.getJobkeeper().getData(
                                            taskNodeZK, true, null);
                                    TaskNode taskNode = (TaskNode) SerializableUtil
                                            .readObject(tdata);
                                    if (taskNode.getJobZkNameList().contains(jobZKName)) {
                                        taskNode.getJobZkNameList().remove(jobZKName);
                                        tdata = SerializableUtil.writeObject(taskNode);
                                        jkWatcher.getJobkeeper().setData(taskNodeZK,
                                                tdata, -1);
                                    }
                                    return true;
                                } catch (Exception e) {
                                    LOG.error("try to delete task error", e);
                                }
                                return false;
                            }
                        });
                        
                        
                        return result;
                    } 
                    return false;
                } catch (Exception e) {
                    LOG.error("error", e);
                }
                return false;
            }
        });
        LOG.debug("deleteJob end, the job name is " + jobName);
        return result;
    }

    /**
     * 强制删除
     * @param jobName
     * @return
     */
    public  boolean deleteWithForce(final String jobName) {
        LOG.debug("Method in deleteJob " + jobName);
        final String jobZKName = JKZNodeInfo.ZNODE_JOBS + "/" + jobName;
        
        LockService lockService = new LockService();
        Boolean result = (Boolean) lockService.doInLock(jobName, new LockCallback() {
            public Object callback(JKWatcher jkWatcher) {
                try {
                    byte[] data = jkWatcher.getJobkeeper().getData(jobZKName, false,
                            null);
                    Job job = (Job) SerializableUtil.readObject(data);
                    LOG.debug("job status is " + job.getStatus());
                        final String procName = job.getProcHostName();
                        LOG.debug("start to delete " + job.getName());
                        jkWatcher.getJobkeeper().delete(jobZKName, -1);
                        
                        LockService lockService = new LockService();
                        Boolean result = (Boolean) lockService.doInLock(procName, new LockCallback() {
                            public Object callback(JKWatcher jkWatcher) {
                                try {
                                    String taskNodeZK = "/jk/slaves/" + procName;
                                    byte[] tdata = jkWatcher.getJobkeeper().getData(
                                            taskNodeZK, true, null);
                                    TaskNode taskNode = (TaskNode) SerializableUtil
                                            .readObject(tdata);
                                    if (taskNode.getJobZkNameList().contains(jobZKName)) {
                                        taskNode.getJobZkNameList().remove(jobZKName);
                                        tdata = SerializableUtil.writeObject(taskNode);
                                        jkWatcher.getJobkeeper().setData(taskNodeZK,
                                                tdata, -1);
                                    }
                                    return true;
                                } catch (Exception e) {
                                    LOG.error("try to delete task error", e);
                                }
                                return false;
                            }
                        });
                        
                        
                        return result;
                } catch (Exception e) {
                    LOG.error("error", e);
                }
                return false;
            }
        });
        LOG.debug("deleteJob end, the job name is " + jobName);
        return result;
    }
    /**
     * 获取所有job列表
     * 
     * @return
     */
    public String getJobs() {
        JSONObject object = null;
        JKWatcher jkWatcher = new JKWatcher(Properties.getInstance());
        List<String> jobNames;
        object = new JSONObject();
        try {
            jobNames = jkWatcher.getJobkeeper().getChildren(
                    JKZNodeInfo.ZNODE_JOBS, false);
            List<Job> jobList = new ArrayList<Job>();
            for (String name : jobNames) {
                byte[] data = jkWatcher.getJobkeeper().getData(
                        JKZNodeInfo.ZNODE_SLAVES + "/" + name, false, null);
                Job job = (Job) SerializableUtil.readObject(data);
                if (job != null) {
                    jobList.add(job);
                }
            }

            for (Job job : jobList) {
                JSONObject obj = new JSONObject();
                obj.put("jobName", job.getName());
                obj.put("status", job.getStatus());
                obj.put("procHostName", job.getProcHostName());
                object.put(job.getName(), obj);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (jkWatcher != null) {
                jkWatcher.close();
            }
        }
        return object.toString();
    }

    /**
     * 获取所有状态为running的job
     * 
     * @return
     */
    public String getRunningJobs() {
        LOG.debug("Method in getRunningJobs ");
        List<String> jobNames;
        JSONObject object = new JSONObject();
        JKWatcher jkWatcher = null;
        try {
            jkWatcher = new JKWatcher(Properties.getInstance());
            jobNames = jkWatcher.getJobkeeper().getChildren(
                    JKZNodeInfo.ZNODE_JOBS, false);
            List<Job> jobList = new ArrayList<Job>();
            for (String name : jobNames) {
                byte[] data = jkWatcher.getJobkeeper().getData(
                        JKZNodeInfo.ZNODE_JOBS + "/" + name, false, null);
                Job job = (Job) SerializableUtil.readObject(data);
                if (job != null) {
                    if (job.getStatus() == Status.RUNNING) {
                        jobList.add(job);
                    }
                }
            }

            for (Job job : jobList) {
                JSONObject obj = new JSONObject();
                obj.put("jobName", job.getName());
                obj.put("status", Status.RUNNING);
                object.put(job.getName(), obj);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (jkWatcher != null) {
                jkWatcher.close();
            }
        }
        return object.toString();
    }

    /**
     * 获取job的状态信息
     * 
     * @param jobName
     * @return
     */
    public int getJobStatus(String jobName) {
        LOG.debug("Method in getJobStatus " + jobName);
        int ret = Message.JOBNOTEXISTS;
        byte[] data = null;
        JKWatcher jkWatcher = null;
        try {
            jkWatcher = new JKWatcher(Properties.getInstance());
            data = jkWatcher.getJobkeeper().getData(
                    JKZNodeInfo.ZNODE_JOBS + "/" + jobName, false, null);
            Job job = (Job) SerializableUtil.readObject(data);
            if (job != null) {
                return job.getStatus();
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (jkWatcher != null) {
                jkWatcher.close();
            }
        }
        return ret;
    }

    /**
     * 获取job的详细信息
     * 
     * @param jobName
     * @return
     */
    public byte[] getJobDetailInfo(String jobName) {
        byte[] data = null;
        JKWatcher jkWatcher = null;
        try {
            jkWatcher = new JKWatcher(Properties.getInstance());
            data = jkWatcher.getJobkeeper().getData(
                    JKZNodeInfo.ZNODE_JOBS + "/" + jobName, false, null);
            return data;
        } catch (KeeperException e) {
            LOG.error("getJobDetailInfo KeeperException " + e);
            if (e.code() == Code.NONODE) {
                return null;
            }
        } catch (InterruptedException e) {
            LOG.error("getJobDetailInfo InterruptedException " + e);
        } finally {
            if (jkWatcher != null) {
                jkWatcher.close();
            }
        }
        return null;
    }

    /**
     * 获取job的proc信息
     * 
     * @param jobName
     * @return
     */
    public String getProcStatus(String jobName) {
        LOG.info("getProcStatus " + jobName);
        String msg = null;
        byte[] data = null;
        JKWatcher jkWatcher = null;
        try {
            jkWatcher = new JKWatcher(Properties.getInstance());
            data = jkWatcher.getJobkeeper().getData(
                    JKZNodeInfo.ZNODE_JOBS + "/" + jobName, false, null);
            Job job = (Job) SerializableUtil.readObject(data);
            if (job != null) {
                msg = (String) job.getConfiguration().getArgs().get("msg");
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (jkWatcher != null) {
                jkWatcher.close();
            }
        }
        return msg;
    }

   

}