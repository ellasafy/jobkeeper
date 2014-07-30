package com.cstor.utils;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
public class JKZooKeeper {
  private static final Log LOG = LogFactory.getLog(JKZooKeeper.class);
  private ZooKeeper zk;
  private final RetryCounterFactory retryCounterFactory;
  public JKZooKeeper(String quorumServers, int seesionTimeout,Watcher watcher, int maxRetries, int retryIntervalMillis) throws IOException {
    this.zk = new ZooKeeper(quorumServers, seesionTimeout, watcher);
    this.retryCounterFactory = new RetryCounterFactory(maxRetries, retryIntervalMillis);
  }

  public void delete(String path, int version) throws InterruptedException, KeeperException {
    RetryCounter retryCounter = retryCounterFactory.create();
    boolean isRetry = false; // False for first attempt, true for all retries.
    while (true) {
      try {
        zk.delete(path, version);
        return;
      } catch (KeeperException e) {
        switch (e.code()) {
          case NONODE:
            if (isRetry) {
              LOG.info("Node " + path + " already deleted. Assuming that a previous attempt succeeded.");
              return;
            }
            LOG.warn("Node " + path + " already deleted, and this is not a retry");
            throw e;

          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper delete failed after " + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
      isRetry = true;
    }
  }

  public Stat exists(String path, Watcher watcher) throws KeeperException, InterruptedException {
	RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return zk.exists(path, watcher);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper exists failed after " + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return zk.exists(path, watch);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper exists failed after " + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  public List<String> getChildren(String path, Watcher watcher)throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return zk.getChildren(path, watcher);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper getChildren failed after " + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return zk.getChildren(path, watch);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper getChildren failed after " + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  public byte[] getData(String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return zk.getData(path, watcher, stat);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper getData failed after "  + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;
          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  public byte[] getData(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    while (true) {
      try {
        return zk.getData(path, watch, stat);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper getData failed after " + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;

          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }

  public Stat setData(String path, byte[] data, int version)throws KeeperException, InterruptedException {
    RetryCounter retryCounter = retryCounterFactory.create();
    byte[] newData = data;
    while (true) {
      try {
        return zk.setData(path, newData, version);
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            if (!retryCounter.shouldRetry()) {
              LOG.error("ZooKeeper setData failed after " + retryCounter.getMaxRetries() + " retries");
              throw e;
            }
            break;
          case BADVERSION:
            // try to verify whether the previous setData success or not
          default:
            throw e;
        }
      }
      retryCounter.sleepUntilNextRetry();
      retryCounter.useRetry();
    }
  }
  public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode)throws KeeperException, InterruptedException {
    byte[] newData = data;
    switch (createMode) {
      case EPHEMERAL:
      case PERSISTENT:
        return createNonSequential(path, newData, acl, createMode);
      case EPHEMERAL_SEQUENTIAL:
      case PERSISTENT_SEQUENTIAL:
      default:
        throw new IllegalArgumentException("Unrecognized CreateMode: " + 
            createMode);
    }
  }
  private String createNonSequential(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException {
	    RetryCounter retryCounter = retryCounterFactory.create();
	    boolean isRetry = false; // False for first attempt, true for all retries.
	    while (true) {
	      try {
	        return zk.create(path, data, acl, createMode);
	      } catch (KeeperException e) {
	        switch (e.code()) {
	          case NODEEXISTS:
	            if (isRetry) {
	              byte[] currentData = zk.getData(path, false, null);
	              if (currentData != null &&
	                  Bytes.compareTo(currentData, data) == 0) { 
	                // We successfully created a non-sequential node
	                return path;
	              }
	              LOG.error("Node " + path + " already exists with " +  Bytes.toStringBinary(currentData) + ", could not write " +  Bytes.toStringBinary(data));
	              throw e;
	            }
	            LOG.error("Node " + path + " already exists and this is not a retry");
	            throw e;

	          case CONNECTIONLOSS:
	          case OPERATIONTIMEOUT:
	            LOG.warn("Possibly transient ZooKeeper exception: " + e);
	            if (!retryCounter.shouldRetry()) {
	              LOG.error("ZooKeeper create failed after " + retryCounter.getMaxRetries() + " retries");
	              throw e;
	            }
	            break;

	          default:
	            throw e;
	        }
	      }
	      retryCounter.sleepUntilNextRetry();
	      retryCounter.useRetry();
	      isRetry = true;
	    }
	  }
	public long getSessionId() {
		return zk.getSessionId();
	}

	public void close() throws InterruptedException {
		zk.close();
	}

	public States getState() {
		return zk.getState();
	}

	public ZooKeeper getZooKeeper() {
		return zk;
	}

	public byte[] getSessionPasswd() {
		return zk.getSessionPasswd();
	}

	public void sync(String path, AsyncCallback.VoidCallback cb, Object ctx) {
		this.zk.sync(path, null, null);
	}

}
