package edu.sjsu.cs249.zooqueue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZooQueueInitial implements Watcher {

	@Override
	public void process(WatchedEvent arg0) {
		// TODO Auto-generated method stub
		
	}
	
//	/*connection string containing a comma separated list of host:port pairs, 
//	 * each corresponding to a ZooKeeper server
//	*/
//	private String serverEnsemble; 
//	private String queuePath;
//	private String workItemPath;
//	private String resultsPath;
//	public String agentName;
//	private ZooKeeper zk = null;
//	private WorkItemProcessor wip = null;
//	
//	Object lock1 = new Object();
//	
//	public ZooQueueInitial(String serverEnsemble,
//			String queuePath,
//			String workItemPath,
//			String resultsPath,
//			String agentName) throws IOException {
//		//TODO: addInputValidations
//		this.serverEnsemble = serverEnsemble;
//		this.queuePath = queuePath;
//		this.workItemPath = workItemPath;
//		this.resultsPath = resultsPath;
//		this.agentName = agentName;
//		
//		//create zookeeper object, i think ..this itself establishes the connection..
//		zk = new ZooKeeper(this.serverEnsemble, 3000, this);
//		System.out.println(agentName+ " : "+ " Connection established");
//		System.out.println(agentName+ " : "+ " sessionID : " + zk.getSessionId());
//		this.wip = new WorkItemProcessor(zk, workItemPath, resultsPath, agentName);
//	}
//	
//	//TODO: Lifecycle function...start... process stuff until shutdown...exit
//	
//	public void beginLifeCycle() throws KeeperException, InterruptedException
//	{
//		queueInForWork();
//		
//		zk.close();
//	}
//	
//	public void queueInForWork() throws KeeperException, InterruptedException
//	{
//		/*
//		 * 1. Create znode under /queue
//        if(/queue contains only 1 znode i.e the current znode)
//        {
//           //call processItem()
//        }
//        else 
//        {
//            1.list the znodes under /queue
//            2.Find the znode that has next least to the current znode aka nextInLine
//            3."Watch" on nextInLine for it to be deleted.
//            4. Once a deleted watcher notification is recieved.. //call processItem()
//        }
//		 * */
//		String worker = zk.create(queuePath+"/"+ agentName +"-" , "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
//		Stat queueStat = new Stat();
//		//TODO: for now assuming that we get the absolute paths of all the children.
//		List<String> childrenNames = zk.getChildren(queuePath, false, queueStat);
//		
//		if(queueStat.getNumChildren() == 1)
//		{
//			System.out.println(agentName+ " : "+"I am the only one here...ha ha ha...processing the workItem...");
//			wip.processWorkItem();
//		}
//		else
//		{
//			System.out.println(childrenNames);
//			String precedingNode = getPrecedingNodeName(worker, childrenNames);
//			System.out.println(precedingNode);
//			System.out.println(agentName+ " : "+"Damnn...There are other wokers in line..I gotta wait.. for " + precedingNode + " to be deleted...");
//			zk.getData(queuePath + "/" +precedingNode, true, new Stat());
//			//wait here..until you get notified.
//			synchronized(lock1)
//			{
//				System.out.println(agentName+ " : "+"Begin wait");
//				lock1.wait();
//				System.out.println(agentName+ " : "+"End wait");
//			}
//			System.out.println(agentName+ " : "+"Finally....I can now process a work Item....processig it");
//			wip.processWorkItem();
//			System.out.println(agentName+ " : "+"done.....");
//		}
//		
//		//have a boolean that checks if you have processed succesfully and delte..
//		System.out.println(agentName+ " : "+"Deleting...." + worker);
//		zk.delete(worker, -1);
//		if(zk.exists(worker, false) == null)
//		{
//			System.out.println(agentName+ " : "+worker + " deleted...");
//		}
//		
//	}
//	
//	private String getPrecedingNodeName(String worker, List<String> childrenNames)
//	{
//		//assuming that all agents will have the following name format "<agent_name>-<sequentialNumber>"
//		// also assuming that there will be only -
////		int targetSeqNum = Integer.parseInt(worker.substring(worker.indexOf('-') + 1)) - 1;
////		for(String child: childrenNames)
////		{
////			int start = child.indexOf('-') + 1;
////			int seqNum =  Integer.parseInt(child.substring(start));
////			if(targetSeqNum == seqNum )
////			{
////				return child;
////			}
////		}
//		
//		int secondMax = 0;//Integer.parseInt(childrenNames.get(0).substring(childrenNames.get(0).indexOf('-') + 1));
//		String secondMaxS = "";childrenNames.get(0);
//		int workerSeqNum = Integer.parseInt(worker.substring(worker.indexOf('-') + 1));
//		HashMap<Integer, String> seqMap = new HashMap<>();
//		List<Integer> seqNums = new ArrayList<>();
//		for(int i = 1; i < childrenNames.size(); i++)
//		{
//			String child = childrenNames.get(i);
//			int start = child.indexOf('-') + 1;
//			int seqNum =  Integer.parseInt(child.substring(start));
//			seqMap.put(seqNum, child);
//			seqNums.add(seqNum);
//		}
//		Collections.sort(seqNums);
//		
//		return seqMap.get(seqNums.get(seqNums.size()-2));
//	}
//	
//	@Override
//	public void process(WatchedEvent arg0) {
//		System.out.println(agentName+ " : "+"Finally ..." + arg0.getPath()+ " got deleted...lifting wait ..") ;
//		if(arg0.getType() == Watcher.Event.EventType.NodeDeleted)
//		{
//			//assuming getPath() returns the name of the deleted node
//			System.out.println(agentName+ " : "+"Finally ..." + arg0.getPath()+ " got deleted...lifting wait ..") ;
//			synchronized (lock1) {
//				lock1.notify();
//			}
//		}
//		
//	}
//	public String getServerEnsemble() {
//		return serverEnsemble;
//	}
//	public void setServerEnsemble(String serverEnsemble) {
//		this.serverEnsemble = serverEnsemble;
//	}
//	public String getQueuePath() {
//		return queuePath;
//	}
//	public void setQueuePath(String queuePath) {
//		this.queuePath = queuePath;
//	}
//	public String getWorkItemPath() {
//		return workItemPath;
//	}
//	public void setWorkItemPath(String workItemPath) {
//		this.workItemPath = workItemPath;
//	}
//	public String getResultsPath() {
//		return resultsPath;
//	}
//	public void setResultsPath(String resultsPath) {
//		this.resultsPath = resultsPath;
//	}
//	public String getAgentName() {
//		return agentName;
//	}
//	public void setAgentName(String agentName) {
//		this.agentName = agentName;
//	}
//	
//	public static void main(String[] args) throws IOException, KeeperException, InterruptedException
//	{
//		String prefix = "";
//		ZooQueueInitial zqi = new ZooQueueInitial("localhost:2181", prefix+"/queue", prefix+"/workItems", prefix+"/results", "Lazarus");
//		zqi.beginLifeCycle();
//	}
//	
}
