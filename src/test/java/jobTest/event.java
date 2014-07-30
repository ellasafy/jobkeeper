package jobTest;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class event {
    private static final Logger LOG = LoggerFactory.getLogger(event.class);
    public static void main(String[] args) {
        String hostName = "192.168.2.121";
        String jobZkName = "/jk/job/192.168.2.141****500_decode_copy";
        String rpcMethod = "EVENTHANDLECLASS.dataChangeEvent";
        try {
            String url = "http://" + hostName + ":6666";
            XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
            config.setServerURL(new URL(url));
            XmlRpcClient client = new XmlRpcClient();
            List<Object> argss = new ArrayList<Object>();
            argss.add(jobZkName);
            client.setConfig(config);
           String re =  (String)client.execute("EVENTHANDLECLASS.dataChangeEvent", argss);
           System.out.println(re);
        } catch (MalformedURLException e) {
            LOG.error("rpc exception ", e);
        } catch (XmlRpcException e) {
            LOG.error("rpc exception ", e);
        }
    }

}
