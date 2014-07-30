package com.cstor.slaves.bean;

public interface Message {
	public static final int SUCCESS = 1111;
	public static final int JOBEXISTS = 1000;
	public static final int JOBERROR = 1001;
	public static final int JOBKEEPEREXCEPTION = 1011;
	public static final int JOBINTERRUPTEDEXCEPTION = 1012;
	public static final int TASKNODEJOBEXISTS = 1013;
	public static final int JOBSUCCESS = 1002 ;
	public static final int JOBNOTEXISTS = 1003;
	public static final int JOBADDSUCCESS = 2001;
	public static final int JOBSTOPSUCCESS = 3001;
	public static final int JOBSTOPERROR = 3002;
	public static final int TASKNODENOTEXISTS = 4001;
	public static final int TASKNODEERROR = 4002;
}
