CS249 – Distributed Computing – Project  

We implemented a workqueue with ZooKeeper. Each student has written an agent. when that agent runs, it will insert itself into a work queue. when it is the agent's turn to run, it will take a work item, process it, allow the next agent to run, and go to the end of the queue.

I developed ZooQueue to take 3 parameters when it runs: 1) the location of the ZooKeeper ensemble, 2) the path to the queue datastructure, and 3) the name of the agent.

There are 3 important znodes used by ZooQueue: /queue this znode has an ordered list of ephemeral-sequential children znodes that defines the queue; /workitems this znode has an ordered list of sequential znodes that contain work items; /results is a znode that has an ordered list of sequential znodes containing results.
queuing

when a ZooQueue agent starts up, it gets in the queue by creating an emphemeral-sequential znode under /queue. it should use its name as the name of the znode. if it has the lowest sequence number it processes a work item, otherwise it waits for the znode with the next lowest sequence number to disappear.

<b>processing</b></n>
When the ZooQueue agent is ready to process a work item, it will look in the /workitems znode for an item to process. it should choose the item with the lowest sequence number to process. if there are not items to process, it must wait for an item to appear.
the agent processes an item by: 1) removing the item for /workitems, 2) creating a sequential znode under results with the name of the agent and the content being "name_of_agent processed name_of_item_znode".
once the agent has finished processing it will remove its znode under /workqueue and start queuing again.

<b>run the server with:</b>
 java -jar zookeeper-dev-fatjar.jar server 2181 tmpdir
where tmpdir is a directory that you have created to store the data. once the server is started, you can use a command line client with:
java -jar zookeeper-dev-fatjar.jar client -server 127.0.0.1:2181
using the command line client you can create the initial znodes using:

create /queue<br>
create /workitems<br>
create /results<br>
ls /<br>
