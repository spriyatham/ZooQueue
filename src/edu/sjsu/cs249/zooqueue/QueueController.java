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

public class QueueController implements Watcher, Runnable{
	Configuration config = null;
	Object lock1 = new Object();
	boolean shutdown = false;
	boolean restart = false;
	ZooKeeper zk = null;
	ZooQueue zq = null;
	WorkItemProcessor wip;
	public QueueController(Configuration config, ZooKeeper zk, ZooQueue zq) {
		this.config = config;
		this.zk = zk;
		this.wip = new WorkItemProcessor(zk, config.getWorkItemPath(), config.getResultsPath(), config.getAgentName(),this);
		this.zq = zq;
	} 
	
	@Override
	public void process(WatchedEvent arg0) {
		//TODO: Check for session...loss event and add the watcher events..
		//System.out.println(config.getAgentName()+ " : "+"Finally ..." + arg0.getPath()+ " got deleted...lifting wait ..") ;
		System.out.println("Recieved a watch event ..event Type" +  arg0.getState().name());
		if(arg0.getState() == Watcher.Event.KeeperState.Disconnected)
		{
			System.out.println("QueueController : Disconnectedd......");
			System.out.println("QueueController : SessionID = " + zk.getSessionId() + " Original Session ID = " + config.getSessionId());
		}
		if(arg0.getState() == Watcher.Event.KeeperState.SyncConnected)
		{
			System.out.println("QueueController : Connected.........");
			System.out.println("QueueController : SessionID = " + zk.getSessionId() + " Original Session ID = " + config.getSessionId());
		}
		if(arg0.getState() == Watcher.Event.KeeperState.Expired)
		{
			System.out.println("QueueController : Connection Expired...set restart..");
			restart = true;
			synchronized (lock1) {
				lock1.notify();
			}
		}
		if(arg0.getType() == Watcher.Event.EventType.NodeDeleted)
		{
			//assuming getPath() returns the name of the deleted node
			System.out.println(config.getAgentName()+ " : "+"Finally ..." + arg0.getPath()+ " got deleted...lifting wait ..") ;
			synchronized (lock1) {
				lock1.notify();
			}
		}
		
	}

	@Override
	public void run() {
		while(!shutdown) //|| restart
		{
			runningLoop();
			if(restart)
			{
				//connection is already closed...
				try {
					zq.connect(config.getZkEnsemble());
					this.zk = zq.zk;
					this.wip.zk = this.zk;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				restart = false;
				
			}
		}
	}
	
	void runningLoop()
	{
		while(!(shutdown || restart))
		{
			try {
				queueInForWork();
			} catch (KeeperException e) {
				e.printStackTrace();
				if(e instanceof KeeperException.ConnectionLossException)
				{
					System.out.println("Encountered a connection loss exception...restarting." );
					restart = true;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		if(shutdown)
		System.out.println(config.getAgentName() + ": Shutdown acknowledged, exiting...");
		if(restart)
		{
			System.out.println(config.getAgentName() + ": Restart acknowledged, exiting...");
		}
		try {
			System.out.println(config.getAgentName() + ": BEGIN: zk.close()");
			zk.close();
			zk = null;
			System.out.println(config.getAgentName() + ": END: zk.close()");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public void queueInForWork() throws KeeperException, InterruptedException
	{
		/*
		 * 1. Create znode under /queue
        if(/queue contains only 1 znode i.e the current znode)
        {
           //call processItem()
        }
        else 
        {
            1.list the znodes under /queue
            2.Find the znode that has next least to the current znode aka nextInLine
            3."Watch" on nextInLine for it to be deleted.
            4. Once a deleted watcher notification is recieved.. //call processItem()
        }
		 * */
		String queuePath = config.getQueuePath();
		String agentName = config.getAgentName();
		deleteIfExisits();
		String worker = zk.create(queuePath+"/"+ agentName +"-" , "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		Stat queueStat = new Stat();
		//TODO: for now assuming that we get the absolute paths of all the children.
		List<String> childrenNames = zk.getChildren(queuePath, false, queueStat);
		String precedingNode = getPrecedingNodeNameV2(worker, childrenNames);
		if(queueStat.getNumChildren() == 1 || precedingNode.isEmpty()) // || getPrecedingNodeName().isEmpty()
		{
			System.out.println(agentName+ " : "+"I am the first one here...ha ha ha...processing the workItem...");
			wip.processWorkItem();
			//TODO: if restart return
			if(restart) 
			{
				System.out.println(agentName+ " : "+" QueueController : WorkItem processor requested restart..");
				return;
			}
			
		}
		else
		{
			System.out.println(childrenNames);
			//String precedingNode = getPrecedingNodeNameV2(worker, childrenNames);
			System.out.println(precedingNode);
			System.out.println(agentName+ " : "+"Damnn...There are other wokers in line..I gotta wait.. for " + precedingNode + " to be deleted...");
			System.out.println(agentName+ " : "+"Attempting To set a watch..");
			if(setWatch(queuePath, precedingNode, zk, worker))
			{
				System.out.println(agentName+ " : "+"Watch Successfully set...");
				//wait here..until you get notified.
				synchronized(lock1)
				{
					System.out.println(agentName+ " : "+"Begin wait");
					lock1.wait();
					System.out.println(agentName+ " : "+"End wait");
					if(restart) return;
				}
				System.out.println(agentName+ " : "+"Finally....I can now process a work Item....processig it");
			}
			else 
			{
				System.out.println("Could not / Need not set a watch for some weird reasond...proceeding to process work item..");
			}
			wip.processWorkItem();
			if(restart) 
			{
				System.out.println(agentName+ " : "+" QueueController : WorkItem processor requested restart..");
				return;
			}
			System.out.println(agentName+ " : "+"done.....");
		}
		
		//have a boolean that checks if you have processed succesfully and delte..
		System.out.println(agentName+ " : "+"Deleting...." + worker);
		zk.delete(worker, -1);
		if(zk.exists(worker, false) == null)
		{
			System.out.println(agentName+ " : "+worker + " deleted...");
		}
		
	}
	
	private boolean setWatch(String queuePath, String precedingNode, ZooKeeper zk, String worker) throws InterruptedException, KeeperException
	{
		try
		{
			zk.getData(queuePath + "/" +precedingNode, this, new Stat());
		}
		catch(KeeperException ke)
		{
			if(ke instanceof KeeperException.NoNodeException)
			{
				System.out.println("The selected preceding node" + queuePath + "/" +precedingNode + " does not exist..Hence trying to get a another node to watch on" );
				Stat queueStat = new Stat();
				List<String> childrenNames = zk.getChildren(queuePath, false, queueStat);
				if(childrenNames.size() == 1) return false;
				String precedingNodeNew = getPrecedingNodeNameV2(worker, childrenNames);
				if(precedingNodeNew.isEmpty()) return false;
				return setWatch(queuePath, precedingNodeNew, zk, worker);
			}//also handle watch not set exception.
			else
			{
				throw ke;
			}
		}
		return true;
	}
	
	private String getPrecedingNodeName(String worker, List<String> childrenNames)
	{
		//assuming that all agents will have the following name format "<agent_name>-<sequentialNumber>"
		// also assuming that there will be only -
		HashMap<Integer, String> seqMap = new HashMap<>();
		List<Integer> seqNums = new ArrayList<>();
		for(int i = 0; i < childrenNames.size(); i++)
		{
			String child = childrenNames.get(i);
			int start = child.indexOf('-') + 1;
			int seqNum =  Integer.parseInt(child.substring(start));
			seqMap.put(seqNum, child);
			seqNums.add(seqNum);
		}
		//TODO: Handle situation in which I am already in the queue...
		Collections.sort(seqNums);
		if(seqNums.size() == 1)
		{
			//By doing this..in a corner case..when we query for children again...and all the other children before the
			return worker;
		}
		
		return seqMap.get(seqNums.get(seqNums.size()-2));
	}
	
	private String getPrecedingNodeNameV2(String worker, List<String> childrenNames)
	{
		//assuming that all agents will have the following name format "<agent_name>-<sequentialNumber>"
		// also assuming that there will be only -
		HashMap<Integer, String> seqMap = new HashMap<>();
		int workerSeqNum = -1;
		List<Integer> seqNums = new ArrayList<>();
		for(int i = 0; i < childrenNames.size(); i++)
		{
			String child = childrenNames.get(i);
			int length = child.length();
			int start = child.indexOf('-') + 1;
			int seqNum =  Integer.parseInt(child.substring(length-10));
			if(worker.contains(child)) workerSeqNum = seqNum;
			seqMap.put(seqNum, child);
			seqNums.add(seqNum);
		}
		//TODO: Handle situation in which I am already in the queue...
		Collections.sort(seqNums);
		int wsi = seqNums.indexOf(workerSeqNum);
		if(wsi == 0) return "";
		return seqMap.get(seqNums.get(wsi-1));
	}
	public void shutdown()
	{
		System.out.println(config.getAgentName()+ " : QueueController : Setting shutdown to true");
		this.shutdown = true;
	}
	
	void deleteIfExisits() throws KeeperException, InterruptedException
	{
		List<String> childrenNames = zk.getChildren(config.queuePath, false, new Stat());
		String agentName = config.agentName;
		for(String child: childrenNames)
		{
			if(child.contains(agentName))
			{
				zk.delete(config.queuePath + "/" + child, -1);
				if(zk.exists(config.queuePath + "/" + child, false) == null)
				{
					System.out.println(agentName+ " : "+child + " deleted...");
				}
			}
		}
	}
}

