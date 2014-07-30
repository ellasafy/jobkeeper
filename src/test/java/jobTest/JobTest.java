package jobTest;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;

import com.cstor.slaves.bean.Message;
import com.cstor.userInterface.Configuration;
import com.cstor.userInterface.Job;
import com.cstor.userInterface.Status;
import com.cstor.utils.SerializableUtil;

public class JobTest {
	//调用的远程rpc名称
	private static final String ipcName = "JOBSERVICECLASS.addJob";
	private static final String ip = "192.168.254.128";
	private static final int port = 6666;
	private static final String jobName = "192.168.2.143****502_decode_copy";
	private static final String params = "/cVideo/Recodec/JKJobProc 192.168.2.143****502_decode_copy /cVideo/Recodec/Recodec /cVideo/cStor/cVideo/RTStream/cam502_url2_copy.sdp /usr/sbin/DHReStream 192.168.2.143 rtsp://192.168.2.121:554/RTStream/cam502_url2_copy.sdp";
	
	public static void main(String[] args) {
		JobTest test = new JobTest();
		test.createJob(jobName, params, "", ip, port);
	}
	
	
	public  int createJob(String jobName, String param, String filePath, String ip, int port)
	{
		System.out.println("-------------------createJob() begin!");
		// 任务配置
		Configuration jobConfiguration = new Configuration();

		jobConfiguration.setJobProcClassName("com.cstor.service.RtsJobService");
		jobConfiguration.getArgs().put("RUNNING_SECOND", "/cVideo/Recodec/Recodec");
		jobConfiguration.getArgs().put("RUNNING_TOP", "");
		jobConfiguration.getArgs().put("SDPFILE", "");

		jobConfiguration.getArgs().put("RUNNING_ARGS", param);
		jobConfiguration.getArgs().put("RUNNING_FLAG", Status.INIT);
		
	
		// 创建Job,并序列化
		//设置开始时间小于jk的时间
		long less = 110000000000L;
		Job job = new Job(1, System.currentTimeMillis() - less, System.currentTimeMillis() - less, -1l, jobName, null, jobConfiguration);

		byte[] data = SerializableUtil.writeObject(job);
		// 客户端访问
		XmlRpcClientConfigImpl xmlRpcClientConfig = new XmlRpcClientConfigImpl();
		XmlRpcClient xmlRpcClient = new XmlRpcClient();
		xmlRpcClient.setConfig(xmlRpcClientConfig);
		int flag = Message.JOBERROR;

		try
		{
			System.out.println("getEnableJKIp before!");
			System.out.println("createJob() accessMaster:"+ip+ ",port:" + port);
			xmlRpcClientConfig.setServerURL(new URL("http://" + ip + ":" + port));
			List<Object> rpcArgs = new ArrayList<Object>();
			rpcArgs.add(data);
			Object resultObj = (Object) xmlRpcClient.execute(ipcName, rpcArgs);//why failure
				String resultJsonStr = (String) resultObj;//why failure
				JSONObject resultJsonObj = JSONObject.fromObject(resultJsonStr);
				flag = (Integer) resultJsonObj.get("returnCode");
				System.out.println("addJob result:"+flag);
				System.out.println("createRecJob result:" + (flag == Message.JOBADDSUCCESS ? "success" : "failure"));
				
				System.out.println("-------------------createRecJob end !");
			
			return flag;
		} catch (MalformedURLException e)
		{
			e.printStackTrace();
			System.out.println("-------------------createJob() end!");
		} catch (XmlRpcException e)
		{
			e.printStackTrace();
			System.out.println("-------------------createJob() end!");
		}
		return Message.JOBERROR;
	}

}
