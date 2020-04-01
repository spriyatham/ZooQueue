package edu.sjsu.cs249.zooqueue;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
//TODO:
/**
 * 1. Handling server restart when waiting..
 * 2. Handling errors while working..on the stuff.
 * */

public class ZooQueue {
	
	Configuration config;
	ZooKeeper zk;
	
	public ZooQueue(String serverEnsemble,
			String queuePath,
			String workItemPath,
			String resultsPath,
			String agentName) throws IOException, InterruptedException {
		//TODO: addInputValidations
		this.config = new Configuration(serverEnsemble, queuePath, workItemPath, resultsPath, agentName);
		
		connect(serverEnsemble);
		this.config.setSessionTimeout(zk.getSessionTimeout());
		System.out.println(agentName+ " : "+ " Connection established");
		System.out.println(agentName+ " : "+ " sessionID : " + zk.getSessionId());
		System.out.println(agentName+ " : "+ " session timeout : " + zk.getSessionTimeout());
		
	}
		
	public boolean connect(String host) throws IOException,InterruptedException {
		
		CountDownLatch connectedSignal = new CountDownLatch(1);
	
	    zk = new ZooKeeper(host,5000,(Watcher) new Watcher() {
		
	        public void process(WatchedEvent we) {
	            if (we.getState() == KeeperState.SyncConnected) {
               connectedSignal.countDown();
            }
         }
      });
			
      connectedSignal.await();
      
      return true;
	 }
	
	public void close() throws InterruptedException
	{
		zk.close();
	}
	

	public static void main(String[] args) throws IOException, InterruptedException {
		ZooQueue zooQueue = new ZooQueue(args[0], args[1], args[2], args[3], args[4]);
		QueueController queueController = new QueueController(zooQueue.config, zooQueue.zk,zooQueue);
		System.out.println(zooQueue.config.agentName + ": starting QueueController Thread.." );
		Thread qct = new Thread(queueController);
		qct.start();
		
		Scanner s = new Scanner(System.in);
		System.out.println("Type \"quit\" to stop zooqueue...");
		boolean shutdown = false;
		while(!shutdown)
		{
			String input = s.next().strip();
			if(input.equalsIgnoreCase("quit"))
			{
				System.out.println(zooQueue.config.agentName + " : Recieved shutdown request...");
				shutdown = true;
				queueController.shutdown();
			}
		}
		
	}

}
