package edu.sjsu.cs249.zooqueue;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

public class WorkItemProcessor implements Watcher {
	ZooKeeper zk;
	String agentName;
	String workItemPath;
	String resultsPath;
	Object lock1 = new Object();
	long orgSessionId;
	QueueController qc;
	public WorkItemProcessor(ZooKeeper zk, String workItemPath, String resultsPath, String agentName, QueueController qc)
	{
		this.zk = zk;
		this.workItemPath = workItemPath;
		this.resultsPath = resultsPath;
		this.agentName = agentName;
		orgSessionId = this.zk.getSessionId();
		this.qc = qc;
	}
	
	boolean processWorkItem() throws KeeperException, InterruptedException
	{
		/**
		 * 1. Pick an item to process
		 *    a. getChildren on /workItems
		 *    b. if(numChildren == 0) wait until notified..
		 *    	 else
		 *    		Pick the least seq child and process.
		 * 2. process
		 * 	  
		 * 	  a. under /results create a sequential znode
		 * 			Name: agentName
		 * 			content: "name_of_agent processed name_of_item_znode".   
		 *    b. delete the selected workitem from /workItems 
		 * */
		List<String> workItems = zk.getChildren(workItemPath, false);
		if(workItems.size() == 0)
		{
			//wait..
			synchronized (lock1) {
				System.out.println(agentName + ":" + " No items process.....I will set watch and wait...");
				zk.getChildren(workItemPath,this);
				lock1.wait();
				System.out.println(agentName + ":" + " Yay!!..wait endend.. Its time for me to process..");
				if(qc.restart)
				{
					System.out.println(agentName + ":" + " restart acknowledged, returning from work processor");
					return false;
				}
			}
			//TODO: Handle size 0 ..here....-- DONE
			workItems = zk.getChildren(workItemPath, false);
			if(workItems.size() == 0) 
			{
				System.out.println(agentName + ":" + " Damn...still no work Items... I will start looking for them again..");
				processWorkItem();
				return true;
			}
			
		}	
		
		String workItem = (workItems.size() == 1)? workItems.get(0) : getWorkItem(workItems);
		
		System.out.println(agentName + ":" + " Ready to process ..." + workItem);
		//processing..
		String content = agentName + " processed " + workItem;
		System.out.println(agentName + " : " +  "creating result node ");
		String resultNode = zk.create(resultsPath+"/"+ agentName +"-" , content.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		System.out.println(agentName + " : " +  " result node " + resultNode + " created...");
		
		try 
		{	
			System.out.println(agentName + " : " + " deleting " + workItemPath + "/" +workItem);
			zk.delete(workItemPath + "/" + workItem, -1);
			System.out.println(agentName + " : " + workItemPath + "/" +workItem + " DELETED");
		}catch(InterruptedException ie)
		{
			System.out.println(agentName + " : " + "delete of "+ workItem +" interrupted....trying again..");
			zk.delete(workItemPath + "/" + workItem, -1);
			System.out.println(agentName + " : " + workItemPath + "/" +workItem + " DELETED");
		}
		
		
		return true;
	}
	
	private String getWorkItem(List<String> childrenNames)
	{
		//assuming that all agents will have the following name format "<agent_name>-<sequentialNumber>"
		// also assuming that there will be only -
		int minSeq = Integer.MAX_VALUE;
		String workItem = "";
		for(String child: childrenNames)
		{
			int start = child.indexOf('-') + 1;
			int seqNum =  Integer.parseInt(child.substring(start));
			if(seqNum < minSeq )
			{
				minSeq = seqNum;
				workItem = child;
			}
		}
		return workItem;
	}
	
	@Override
	public void process(WatchedEvent arg0) {
		// TODO Auto-generated method stub
		System.out.println("Recieved a watch event ..event Type" +  arg0.getState().name());
		if(arg0.getState() == Watcher.Event.KeeperState.Disconnected)
		{
			System.out.println("WorkItemProcessor : Disconnectedd......");
			System.out.println("WorkItemProcessor : SessionID = " + zk.getSessionId() + " Original Session ID = " + orgSessionId);
		}
		if(arg0.getState() == Watcher.Event.KeeperState.SyncConnected)
		{
			System.out.println("WorkItemProcessor : Connected.........");
			System.out.println("WorkItemProcessors : SessionID = " + zk.getSessionId() + " Original Session ID = " + orgSessionId);
		}
		
		if(arg0.getState() == Watcher.Event.KeeperState.Expired)
		{
			System.out.println("WorkItemProcessor : Connection Expired...set restart..");
			qc.restart = true;
			synchronized (lock1) {
				lock1.notify();
			}
		}
		if(arg0.getType() == Watcher.Event.EventType.NodeChildrenChanged)
		{
			System.out.println(agentName + " : " +  "found something to process..notifying");
			synchronized (lock1) {
				lock1.notify();
				System.out.println(agentName + " : " +  "Notfied");
			}
		}
	}

}
