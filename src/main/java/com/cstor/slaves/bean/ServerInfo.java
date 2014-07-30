package com.cstor.slaves.bean;


public class ServerInfo implements Comparable<ServerInfo> {

	private String ip;
	private double cpuused;
	private int totalmem;
	private int memused;
	private double uploadNetBand;
	private double downloadNetBand;
	private double bandwidth;
	private static int BASESCORE = 1000;

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public double getCpuused() {
		return cpuused;
	}

	public void setCpuused(double cpuused) {
		this.cpuused = cpuused;
	}

	public int getTotalmem() {
		return totalmem;
	}

	public void setTotalmem(int totalmem) {
		this.totalmem = totalmem;
	}

	public int getMemused() {
		return memused;
	}

	public void setMemused(int memused) {
		this.memused = memused;
	}

	public double getUploadNetBand() {
		return uploadNetBand;
	}

	public void setUploadNetBand(double uploadNetBand) {
		this.uploadNetBand = uploadNetBand;
	}

	public double getDownloadNetBand() {
		return downloadNetBand;
	}

	public void setDownloadNetBand(double downloadNetBand) {
		this.downloadNetBand = downloadNetBand;
	}

	public double getBandwidth() {
		return bandwidth;
	}

	public void setBandwidth(double bandwidth) {
		this.bandwidth = bandwidth;
	}

	@Override
	public String toString() {
		return "ServerInfo [ip=" + ip + ", cpuused=" + cpuused
				+ ", totalmem=" + totalmem + ", memused=" + memused
				+ ", uploadNetBand=" + uploadNetBand + ", downloadNetBand="
				+ downloadNetBand + ", bandwidth=" + bandwidth + ", score=" + (getCpuScore()+","+getMemScore() +","+ getNetScore()) + "]";
	}

	public int getCpuScore() {
		int score = (int) cpuused;
		return score >= BASESCORE ? BASESCORE : score;
	}

	public int getMemScore() {
		int score = (int) (memused * 100 / totalmem);
		return score >= BASESCORE ? BASESCORE : score;
		}

	public int getNetScore() {
		int score = (int) (((uploadNetBand + downloadNetBand) / bandwidth) * 100);
		return score >= BASESCORE ? BASESCORE : score;
	}

	public int compareTo(ServerInfo si) {
		int socre1 = Math.max(getCpuScore(), Math.max(getMemScore(), getNetScore()));
		int score2 = Math.max(si.getCpuScore(), Math.max(si.getMemScore(), si.getNetScore()));
		int result = socre1 - score2;
		return result > 0 ? 1 : (result == 0 ? 0 : -1);
	}
}
