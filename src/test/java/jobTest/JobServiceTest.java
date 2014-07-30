package jobTest;

import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cstor.JKWatcher;
import com.cstor.JobKeeper;

public class JobServiceTest {
	private static final Logger LOG = LoggerFactory.getLogger(JobServiceTest.class);
	public static void main(String[] args) {
		
	
			String ip = "192.168.254.128";
//			PropertyConfigurator.configure("../log4j.properties");
			System.setProperty("log4j.configuration", "log4j.properties");
	      
			JKWatcher watcher = new JKWatcher(ip, 50000);
			try {
				if (watcher.getJobkeeper().exists("/jk/job/host3", true)) {
					LOG.info("exists");
				} else {
					LOG.info("not");
				}
				watcher.getJobkeeper().delete("/jk/job/host3");
				watcher.getJobkeeper().exists("/jk/job/host3", true);
				watcher.getJobkeeper().create("/jk/job/host3", "".getBytes(), CreateMode.PERSISTENT);
				
			} catch (Exception e) {
				LOG.error("error");
			}
			
//			JobKeeper jk = new JobKeeper();
//			jk.connection(ip, 5000, watcher);
			
//			watcher.close();
	}
}
