package edu.sjsu.cs249.zooqueue;

public class Configuration {
	String  zkEnsemble;
	String queuePath;
	String workItemPath;
	String resultsPath;
	String agentName;
	long sessionId;
	int sessionTimeout;
	
	public Configuration(String zkEnsemble, String queuePath, String workItemPath, String resultsPath, String agentName) {
		
		this.zkEnsemble = zkEnsemble;
		this.queuePath = queuePath;
		this.workItemPath = workItemPath;
		this.resultsPath = resultsPath;
		this.agentName = agentName;
	}
	public String getZkEnsemble() {
		return zkEnsemble;
	}
	public void setZkEnsemble(String zkEnsemble) {
		this.zkEnsemble = zkEnsemble;
	}
	public String getQueuePath() {
		return queuePath;
	}
	public void setQueuePath(String queuePath) {
		this.queuePath = queuePath;
	}
	public String getWorkItemPath() {
		return workItemPath;
	}
	public void setWorkItemPath(String workItemPath) {
		this.workItemPath = workItemPath;
	}
	public String getResultsPath() {
		return resultsPath;
	}
	public void setResultsPath(String resultsPath) {
		this.resultsPath = resultsPath;
	}
	public String getAgentName() {
		return agentName;
	}
	public void setAgentName(String agentName) {
		this.agentName = agentName;
	}
	public long getSessionId() {
		return sessionId;
	}
	public void setSessionId(long sessionId) {
		this.sessionId = sessionId;
	}
	public int getSessionTimeout() {
		return sessionTimeout;
	}
	public void setSessionTimeout(int sessionTimeout) {
		this.sessionTimeout = sessionTimeout;
	}
	
	
	
	
}
