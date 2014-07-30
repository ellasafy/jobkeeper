package com.cstor.slaves.service;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cstor.lock.service.LockCallback;
import com.cstor.lock.service.LockService;
import com.cstor.slaves.bean.ServerInfo;
import com.cstor.slaves.bean.TaskNode;
import com.cstor.utils.JKWatcher;
import com.cstor.utils.JKZNodeInfo;
import com.cstor.utils.Properties;
import com.cstor.utils.SerializableUtil;
public class RPCClient {
	private static final Logger LOG = LoggerFactory.getLogger(RPCClient.class);
	private JKWatcher jKWatcher;
	
	public RPCClient() {
		this.jKWatcher = RPCZooKeeper.getZooKeeper();
	}
	
	
	public String getLowCpuMemPerMachine(){
		String hostName = Properties.getInstance().getHostName();
		List<String> taskNodeZKNames;
		try {
			taskNodeZKNames = jKWatcher.getJKZooKeeper().getChildren(JKZNodeInfo.ZNODE_SLAVES, true);
			String res = hostName;
			if(taskNodeZKNames == null || taskNodeZKNames.size() <= 1){
				return res;
			}
			TreeSet<ServerInfo> treeSet = new TreeSet<ServerInfo>();
			for(String ip : taskNodeZKNames){
				ServerInfo serverInfo = null;
				if(hostName.equals(ip)){
					serverInfo = getLocalServerInfo();
				}else{
					serverInfo = getRemoteServerInfo(ip);
				}
				if(serverInfo != null){
					LOG.info(serverInfo.toString());
					treeSet.add(serverInfo);
				}
			}
			LOG.info(treeSet.toString());
			return treeSet.first().getIp();
		} catch (KeeperException e) {
			e.printStackTrace();
			return hostName;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return hostName;
		}
	} 
	
	private ServerInfo getRemoteServerInfo(String ip) {
		ServerInfo serverInfo = new ServerInfo();
		try {
			String url = "http://" + ip + ":6666";
			XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
			config.setServerURL(new URL(url));
			XmlRpcClient client = new XmlRpcClient();
			List<Object> args = new ArrayList<Object>();
			client.setConfig(config);
			String result = (String) client.execute("MACHINEINFOCLASS.getServerInfo",args);
			serverInfo.setIp(ip);
			parse2ServerInfo(serverInfo,result);
		} catch (MalformedURLException e) {
			e.printStackTrace();
			serverInfo = null;
		} catch (XmlRpcException e) {
			e.printStackTrace();
			serverInfo = null;
		}
		return serverInfo;
	}

	private void parse2ServerInfo(ServerInfo si, String result) {
		String[] ss = result.split(";");
		int count = 0;
		si.setCpuused(Double.parseDouble(ss[count++]));
		si.setTotalmem(Integer.parseInt(ss[count++]));
		si.setMemused(Integer.parseInt(ss[count++]));
		si.setUploadNetBand(Double.parseDouble(ss[count++]));
		si.setDownloadNetBand(Double.parseDouble(ss[count++]));
		si.setBandwidth(Double.parseDouble(ss[count++]));
	}
	
	private ServerInfo getLocalServerInfo() {
		ServerInfo serverInfo = new ServerInfo();
		MachineInfoService machineInfoService = new MachineInfoService();
		LOG.info("getLocalServerInfo");
		serverInfo.setIp(Properties.getInstance().getHostName());
		parse2ServerInfo(serverInfo,machineInfoService.getServerInfo());
		return serverInfo;
	}

	public int getJobCount(final String hostName){
		LockService lockService = new LockService();
//		int result = (Integer) lockService.doInLock(hostName, new LockCallback() {
//						public Object callback(JKWatcher jkWatcher) {
//							int count = -1;
//							String taskNodeZKName = JKZNodeInfo.ZNODE_SLAVES + "/" + hostName;
//							byte[] dataBuffer;
//							try {
//								dataBuffer = jkWatcher.getJKZooKeeper().getData(taskNodeZKName, true, null);
//								TaskNode taskNode = (TaskNode) SerializableUtil.readObject(dataBuffer);
//								if (taskNode != null) {
//									count = taskNode.getJobZkNameList().size();
//								}
//								return count;
//							} catch (KeeperException e) {
//								e.printStackTrace();
//								return count;
//							} catch (InterruptedException e) {
//								e.printStackTrace();
//								return count;
//							}
//						}
//		});
//		return result;
		return 1;
	}
		
}
