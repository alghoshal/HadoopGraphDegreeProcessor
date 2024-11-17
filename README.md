# HadoopGraphDegreeProcessor
A Hadoop 1.0.x legacy code that operates on a Graph. Using the data of the degrees of separation between a set of existing nodes, the code works out degrees of separation between all pairs of nodes. Though from a different era, code's ported her or posterity! 

- Each line of the input text file consists of Node (Main Node) & its Adjacent Nodes along with the degree of separation between them. 
For e.g.: 
	* 1:2,3\~1|4~2|
which indicates that for the Main Node 1, Nodes 2 & 3 are 1-degree separated, while Node 4 is 2-degrees separated.

- From the input data it is further worked out that nodes 2 & 3 are 2-degree neigbours to each other & 3-degree neigbour to node 4. 
The above e.g. yields the following new (derived) degree of separation between nodes:
	* 2:3\~2|4~3
	* 3:2\~2|4~3
	* 4:2,3~2|

- This is done over several M/R jobs. Each gets executed in sequence over the data file output of the previous run to work out the subsequent next higher degree (2nd, 3rd, etc) neigbours of each node.

- Final output of each run yields a text file similar to the input file but having the list of next higher degree nodes.

## Requirements
Hadoop 1.0.x, Java-5+, junit-3.x, mockito-1.8+, commons-configuration, commons-lang, commons-logging
