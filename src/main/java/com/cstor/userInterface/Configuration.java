package com.cstor.userInterface;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Configuration implements Serializable{

	private static final long serialVersionUID = 4000L;
	private List<String> inputFileNames;
	private List<String> outputFileNames;
	private Map<String, Object> args;
	private String jobProcClassName;
	private String taskProcClassName;

	public List<String> getInputFileNames() {
		return inputFileNames;
	}

	public void setInputFileNames(List<String> inputFileNames) {
		this.inputFileNames = inputFileNames;
	}

	public List<String> getOutputFileNames() {
		return outputFileNames;
	}

	public void setOutputFileNames(List<String> outputFileNames) {
		this.outputFileNames = outputFileNames;
	}

	public Map<String, Object> getArgs() {
		return args;
	}

	public void setArgs(Map<String, Object> args) {
		this.args = args;
	}

	public String getJobProcClassName() {
		return jobProcClassName;
	}

	public void setJobProcClassName(String jobProcClassName) {
		this.jobProcClassName = jobProcClassName;
	}

	public String getTaskProcClassName() {
		return taskProcClassName;
	}

	public void setTaskProcClassName(String taskProcClassName) {
		this.taskProcClassName = taskProcClassName;
	}
	
	public Configuration() {
		this.inputFileNames = new ArrayList<String>();
		this.outputFileNames = new ArrayList<String>();
		this.args = new HashMap<String, Object>();
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("args{");
		for(String key : args.keySet()){
			sb.append(key + "=" + args.get(key) + ";");
		}
		 sb.deleteCharAt(sb.length() - 1);
		sb.append("}\n");
		sb.append("jobProcClassName\t : " + jobProcClassName + "\n");
		sb.append("taskProcClassName\t : " + taskProcClassName + "\n");
		return sb.toString();
	}

}
