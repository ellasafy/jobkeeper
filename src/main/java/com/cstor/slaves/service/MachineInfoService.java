package com.cstor.slaves.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cstor.utils.Properties;


public class MachineInfoService {
	private static final Logger LOG = LoggerFactory.getLogger(MachineInfoService.class);
	
	private BufferedReader getInputStream(File scriptFile) throws IOException, InterruptedException {
		List<String> commandList = new ArrayList<String>();
		commandList.add("/bin/bash");
		commandList.add(scriptFile.getCanonicalPath());
		ProcessBuilder processBuilder = new ProcessBuilder(commandList);
		Process process = processBuilder.start();
		process.waitFor();
		BufferedReader br = new BufferedReader(new InputStreamReader(
				process.getInputStream()));
		return br;
	}
	
	private File writeToTmpFile(String prefix, String suffix,List<String> list){
		File scriptFile = null;
		try {
			scriptFile = File.createTempFile(prefix, ".sh");
			PrintWriter writer = new PrintWriter(scriptFile);
			for (String commandLine : list) {
				writer.println(commandLine);
			}
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return scriptFile;
	}
	
	private String execShell(List<String> shells) {
		List<String> commandList = new ArrayList<String>();
		StringBuffer sb = new StringBuffer();
		commandList.addAll(shells);
		File scriptFile = writeToTmpFile("scripts", ".sh", commandList);
		BufferedReader br = null;
		try {
			br = getInputStream(scriptFile);
			String line = null;
			while((line = br.readLine())!= null){
				sb.append(line);
				sb.append("\n");
			}
			if(sb.length() > 0 ) sb.deleteCharAt(sb.length() - 1);
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally{
			scriptFile.delete();
			if(br != null){
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
		}
		return sb.toString();
	}
	private String execShell(String shell) {
		List<String> list = new ArrayList<String>();
		list.add(shell);
		return execShell(list);
	}
	
	private String getNetUsed() {
		String result = "";
		List<String> commandList = new ArrayList<String>();
		commandList.add("date '+%s %N'");
		commandList.add("cat /proc/net/dev | grep eth0 | sed 's=^.*:==' | awk '{ print $1,$9 }'");
		String[] times = new String[2]; 
		String[] nets = new String[2]; 
		try {
			File scriptFile = writeToTmpFile("scripts", ".sh", commandList);
			BufferedReader br = getInputStream(scriptFile);
			times[0] = br.readLine();
			nets[0] = br.readLine();
			br.close();
			Thread.sleep(100);
			br = getInputStream(scriptFile);
			times[1] = br.readLine();
			nets[1] = br.readLine();
			br.close();
			scriptFile.delete();
			
			long[] inData = new long[2];
			long[] outData = new long[2];
			long[] ss = new long[2];
			long[] ns = new long[2];
			ss[0] = Long.parseLong(times[0].split(" ")[0]);
			ss[1] = Long.parseLong(times[1].split(" ")[0]);
			ns[0] = Long.parseLong(times[0].split(" ")[1]);
			ns[1] = Long.parseLong(times[1].split(" ")[1]);

			long time = ((ss[1] - ss[0]) * 1000) + ((ns[1] - ns[0]) /1000 / 1000) ;
			inData[0] = Long.parseLong(nets[0].split(" ")[0]);
			inData[1] = Long.parseLong(nets[1].split(" ")[0]);
			outData[0] = Long.parseLong(nets[0].split(" ")[1]);
			outData[1] = Long.parseLong(nets[1].split(" ")[1]);
			LOG.info("input  kbs --" + (((inData[1] - inData[0]) / time) * 1000 /1024));
			LOG.info("output kbs --" + (((outData[1] - outData[0]) / time) * 1000 /1024));
			result = (((inData[1] - inData[0]) / time) * 1000 /1024) + "\n" + (((outData[1] - outData[0]) / time) * 1000 /1024);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}


	public double getCpuUsePer(){
		double result = 0;
		double beginRunTime = 0;
		double beginAllTime = 0;
		double endRunTime = 0;
		double endAllTime = 0;
		List<String> commandList = new ArrayList<String>();
		commandList.add("user=`cat /proc/stat | head -n 1 | awk '{print $2}'`");
		commandList.add("nice=`cat /proc/stat | head -n 1 | awk '{print $3}'`");
		commandList.add("system=`cat /proc/stat | head -n 1 | awk '{print $4}'`");
		commandList.add("idle=`cat /proc/stat | head -n 1 | awk '{print $5}'`");
		commandList.add("iowait=`cat /proc/stat | head -n 1 | awk '{print $6}'`");
		commandList.add("irq=`cat /proc/stat | head -n 1 | awk '{print $7}'`");
		commandList.add("softirq=`cat /proc/stat | head -n 1 | awk '{print $8}'`");
		commandList.add("let used=$user+$nice+$system+$iowait+$irq+$softirq");
		commandList.add("let total=$used+$idle");
		commandList.add("echo $used $total ");
		try {
			 String data = execShell(commandList);
			 String[] ss = data.split(" ");
			 beginRunTime = Double.parseDouble(ss[0]);
			 beginAllTime = Double.parseDouble(ss[1]);
			 LOG.info("return -- " + data);
			 Thread.sleep(100);
			 data = execShell(commandList);
			 ss = data.split(" ");
			 endRunTime = Double.parseDouble(ss[0]);
			 endAllTime = Double.parseDouble(ss[1]);
			 LOG.info("return -- " + data);
			 
			 result = (endRunTime - beginRunTime) / (endAllTime - beginAllTime);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public double getMemUsed() {
		File file = new File("/proc/meminfo");
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		double[] result = new double[6];
		String str = null;
		StringTokenizer token = null;
		try {
			while ((str != null) && (str = br.readLine()) != null) {
				token = new StringTokenizer(str);
				if (!token.hasMoreTokens())
					continue;
				str = token.nextToken();
				if (!token.hasMoreTokens())
					continue;
				if (str.equalsIgnoreCase("MemTotal:"))
					result[0] = Double.parseDouble(token.nextToken());
				else if (str.equalsIgnoreCase("MemFree:"))
					result[1] =Double.parseDouble(token.nextToken());
				else if (str.equalsIgnoreCase("SwapTotal:"))
					result[2] = Double.parseDouble(token.nextToken());
				else if (str.equalsIgnoreCase("SwapFree:"))
					result[3] = Double.parseDouble(token.nextToken());
				else if(str.equalsIgnoreCase("Buffers:"))
					result[4] = Double.parseDouble(token.nextToken());
				else if(str.equalsIgnoreCase("Cached:"))
					result[5] = Double.parseDouble(token.nextToken());
			}
			return result[0]-result[1]-result[4]-result[5];
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return -1;
	}
	
	public double getMemAll() {
		File file = new File("/proc/meminfo");
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		double[] result = new double[4];
		String str = null;
		StringTokenizer token = null;
		try {
			while ((str != null) && (str = br.readLine()) != null) {
				token = new StringTokenizer(str);
				if (!token.hasMoreTokens())
					continue;
				str = token.nextToken();
				if (!token.hasMoreTokens())
					continue;
				if (str.equalsIgnoreCase("MemTotal:"))
					result[0] = Double.parseDouble(token.nextToken());
				else if (str.equalsIgnoreCase("MemFree:"))
					result[1] = Double.parseDouble(token.nextToken());
				else if (str.equalsIgnoreCase("SwapTotal:"))
					result[2] = Double.parseDouble(token.nextToken());
				else if (str.equalsIgnoreCase("SwapFree:"))
					result[3] = Double.parseDouble(token.nextToken());
			}
			return result[0];
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return -1;
	}
	
	public String getSpaceInfo()
	{
	    List<String> commandList = new ArrayList<String>();
	    commandList.add("df |grep /dev/sda1 | awk '{max=NF;i=1;while(i<max){print $i;i++}}'");
	    try {
            String data = execShell(commandList);
            return data;
	    }
	    catch(Exception e)
	    {
	        e.printStackTrace();
	    }
	    return null;
	}
	
	
	public double getMemPer(){
		return getMemUsed() / getMemAll();
	}
	
	public String getServerState(){
		StringBuffer sb = new StringBuffer();
		DecimalFormat df = new DecimalFormat("#0.00");
		double cpu = getCpuUsePer();
		sb.append(df.format(cpu * 100) + ";");
		String[] mem = execShell("free | grep Mem|awk  '{max=NF;i=1;while(i<max) {print $i;i++}}' ").split("\n");
		sb.append(mem[1] + ";");
		sb.append(mem[2] + ";");
		String[] net = getNetUsed().split("\n");
		sb.append(net[0] + ";");
		sb.append(net[1] );
		return sb.toString();
	}
	
	public String getServerInfo() {
		StringBuffer sb = new StringBuffer();
		DecimalFormat df = new DecimalFormat("#0.00");
		double cpu = getCpuUsePer();
		sb.append(df.format(cpu * 100) + ";");
		String[] mem = execShell("free | grep Mem|awk  '{max=NF;i=1;while(i<max) {print $i;i++}}' ").split("\n");
		sb.append(mem[1] + ";");
		sb.append(mem[2] + ";");
		String[] net = getNetUsed().split("\n");
		sb.append(net[0] + ";");
		sb.append(net[1] + ";");
		sb.append(Properties.getInstance().getBandwidth() + "");
		return sb.toString();
	}
	

}
