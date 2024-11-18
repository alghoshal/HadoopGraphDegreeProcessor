# HadoopGraphDegreeProcessor
A Hadoop 1.0.x legacy code that operates on a Graph. Using the data of the degrees of separation between a set of existing nodes, the code works out degrees of separation between all pairs of nodes. Though from a different era, code's ported her or posterity! 

- Initially, a file containing all 1-deg separated neibours of a node. Each line consists of Node (Main Node) & its 1-deg separated neigbours list. 
For e.g. The following record indicates that for the Main Node 1, Nodes 2 & 3 are 1-degree separated: 
	* 1:2,3\~1|

- From this input data it is worked out that nodes 2 & 3 are 2-degree neigbours to each other. So the output consists of all the 2-degree separated nodes of every node:
	* 2:3\~2
	* 3:2\~2
	
- This output forms the input to the next round of the chain of M/R jobs. The data file output of the previous run is used to work out the subsequent next higher degree (2nd, 3rd, etc) neigbours of each node. Output of each run yields a text file similar to the input file but having the list of next higher degree nodes.

- Final output at the end of all rounds of all M/R jobs is a text file where each row consists of the main node along with all its neigbours (a pipe separated Adjacency List):
	*	1:2,3\~1|4\~2|7,8\~3|....

## Requirements
Hadoop 1.0.x, Java-5+, junit-3.x, mockito-1.8+, commons-configuration, commons-lang, commons-logging
