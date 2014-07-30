package jobTest;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

import com.cstor.JKWatcher;
import com.cstor.slaves.bean.TaskNode;
import com.cstor.utils.Properties;
import com.cstor.utils.SerializableUtil;

public class TestTaskNode {
	private static final String connectString= "192.168.2.31:2181";
	private static final String hostName="192.168.0.132";
	private static final int sessionTimeout=5000;
	private static final int connectionRetryCount=3;
	private static final int clientPoolCount=100;
   
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		  Properties proper = new Properties(connectString, hostName, sessionTimeout,
				 connectionRetryCount, clientPoolCount);
//		System.setProperty("JOBKEEPER.CONF", "conf");
//		TaskNodeBasicService taskNodeBasicService = new TaskNodeBasicService();
//		taskNodeBasicService.cleanSuccessJobInfo("TestJob141", "localhost");
		JKWatcher jkWatcher = new JKWatcher(proper);
//		byte[] data = jkWatcher.getJKZooKeeper().getData("/jk/job/192.168.101.10****1707_rec", false, null);
//		Job taskNode = (Job) SerializableUtil.readObject(data);
//		System.out.println(taskNode);
		jkWatcher.getJobkeeper().delete("/jk/tmp", -1);
		byte[] data2 = jkWatcher.getJobkeeper().getData("/jk/slaves", false, null);
		if (data2 == null) {
			System.out.println("data null");
		} else {
			System.out.println("not");
		}
		jkWatcher.getJobkeeper().create("/jk/path", "/jk/path".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	
//		TaskNode task = (TaskNode) SerializableUtil.readObject(data2);
//		System.out.println(task);
	}
}
