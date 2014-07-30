package com.cstor.master.service;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cstor.JKWatcher;
import com.cstor.master.bean.Server;
import com.cstor.master.bean.ServerStatus;
import com.cstor.slaves.service.SystemInfoService;
import com.cstor.utils.JKZNodeInfo;
import com.cstor.utils.Properties;

public class ServerService {
    private static final Logger LOG = LoggerFactory
            .getLogger(ServerService.class);
    private Properties prop;
    private JKWatcher jkWatcher;

    public ServerService() {
        prop = Properties.getInstance();
        this.jkWatcher = new JKWatcher(prop);
    }
    
    /*
     * 判断某个节点是否存在
     */
    public boolean isExist(String node) {
        try {
            return jkWatcher.getJobkeeper().exists(node, null);
            
        } catch (KeeperException e) {
            LOG.error("KeeperException error", e);
        } catch (InterruptedException e) {
            LOG.error("InterruptedException error", e);
        } catch (NullPointerException e) {
            LOG.error("NULL exception", e);
        }
        
        return false;
    }

    public String getServerList() {
        LOG.info("begin -- getServerList");
        List<Server> servers = new ArrayList<Server>();
        List<String> masterNodes;
        try {
            masterNodes = jkWatcher.getJobkeeper().getChildren(
                    JKZNodeInfo.ZNODE_MASTER, false);
            LOG.info("masterNodes : " + masterNodes);
            if (masterNodes != null) {
                Server server;
                for (String hostName : masterNodes) {
                    server = new Server();
                    server.setId("N/A");
                    server.setServerip(hostName);
                    servers.add(server);
                }
            }
        } catch (KeeperException e) {
            LOG.error("KeeperException error", e);
        } catch (InterruptedException e) {
            LOG.error("InterruptedException error", e);
        }

        return JSONArray.fromObject(servers).toString();
    }

    public String getServerState() {
        LOG.debug("begin - getServerState");
        Map<String, ServerStatus> serverStates = new HashMap<String, ServerStatus>();
        JSONObject object = new JSONObject();
        List<String> masterNodes;
        try {
            masterNodes = jkWatcher.getJobkeeper().getChildren(
                    JKZNodeInfo.ZNODE_MASTER, false);
            LOG.debug("masterNodes : " + masterNodes);
            if (masterNodes != null) {
                ServerStatus status;
                for (String hostName : masterNodes) {
                    if (prop.getHostName().equals(hostName)) {
                        status = getLocalServerState();
                    } else {
                        status = getRemoteServerState(hostName);
                    }
                    serverStates.put(hostName, status);
                }
            }
            for (String key : serverStates.keySet()) {
                object.put(key, JSONObject.fromObject(serverStates).get(key));
            }
        } catch (KeeperException e) {
            LOG.error("KeeperException error", e);
        } catch (InterruptedException e) {
            LOG.error("InterruptedException error", e);
        }

        return object.toString();
    }

    private ServerStatus getRemoteServerState(String hostName) {
        ServerStatus status = new ServerStatus();
        try {
            String url = "http://" + hostName + ":6000";
            XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
            config.setServerURL(new URL(url));
            XmlRpcClient client = new XmlRpcClient();
            List<Object> args = new ArrayList<Object>();
            client.setConfig(config);
            String result = (String) client.execute(
                    "MACHINEINFOCLASS.getServerState", args);
            status.setIp(hostName);
            parse2ServerState(status, result);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (XmlRpcException e) {
            e.printStackTrace();
        }
        return status;
    }

    private ServerStatus getLocalServerState() {
        ServerStatus status = new ServerStatus();
        SystemInfoService infoService = new SystemInfoService();
        LOG.debug("getLocalServerState");
        status.setIp(prop.getHostName());
        parse2ServerState(status, infoService.getServerState());
        return status;
    }

    private void parse2ServerState(ServerStatus status, String result) {
        String[] ss = result.split(";");
        int count = 0;
        status.setCpuused(ss.length > count++ ? ss[count - 1] : "");
        status.setTotalmem(ss.length > count++ ? ss[count - 1] : "");
        status.setMemused(ss.length > count++ ? ss[count - 1] : "");
        status.setUploadNetBand(ss.length > count++ ? ss[count - 1] : "");
        status.setDownloadNetBand(ss.length > count++ ? ss[count - 1] : "");
    }
}
