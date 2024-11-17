package in.inkind.degsep.mr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * All Mappers
 * 
 * @author algo 
 */
public class GraphMappers extends BaseMR {

	/** ------------Degree separation Map/Reduce------------- **/

	/**
	 * First Mapper: To find the Node pairs a given deg. of separation between them.
	 * Outputs all possible Node pairs & the distance between them
	 * 
	 * @author algo
	 */
	public static class GraphDegreeSeparationIdentifierMap extends Mapper<LongWritable, Text, Text, IntWritable> {

		/**
		 * Identifies all possible pairs between nodes separated by the specified
		 * fromDegree & toDegree, from a given node (referred to as the mainNode). For
		 * the following value (where nodes 2 & 3 are 1-deg separated, & node 4 is
		 * 2-deg. separated from the mainNode 1):
		 * 
		 * 		1:2,3~1|4~2| & fromDegree=1, toDegree=3
		 * 
		 * the output will be: 
		 * 		1:2 1 
		 * 		1:3 1 
		 * 		1:4 2 
		 * 		2:4 3 
		 * 		3:4 3
		 * 
		 * @param key
		 * @param value
		 * @param context
		 * @param fromDegree
		 * @param toDegree
		 * @throws IOException
		 * @throws InterruptedException
		 */
		void emitAllNodePairs(LongWritable key, Text value, Context context, int fromDegree, int toDegree)
				throws IOException, InterruptedException {
			String line = value.toString();
			// Main node
			StringTokenizer tokenizer1 = new StringTokenizer(line, DELIM_COLON);
			String nodeMain = tokenizer1.nextToken();

			// Associated nodes
			StringTokenizer tokenizer2 = new StringTokenizer(tokenizer1.nextToken(), DELIM_LINE);
			int counter = 1;
			String fromDegreeAdjNodes = "", toDegreeAdjNodes = "";
			while (tokenizer2.hasMoreTokens()) {
				String theAssociatedNodesStr = tokenizer2.nextToken();
				if (counter == fromDegree) {
					fromDegreeAdjNodes = theAssociatedNodesStr;
				}
				if (counter == toDegree) {
					toDegreeAdjNodes = theAssociatedNodesStr;
				}
				counter++;
			}

			if (!isEmpty(fromDegreeAdjNodes) && !isEmpty(toDegreeAdjNodes)) {
				// Pull out fromDeg. nodes
				StringTokenizer tokenizer3 = new StringTokenizer(fromDegreeAdjNodes, DELIM_TILDE);
				String adjacentNodesFromDeg = tokenizer3.nextToken();
				int distanceFrom = Integer.parseInt(tokenizer3.nextToken());
				IntWritable distanceValFrom = new IntWritable(distanceFrom);
				StringTokenizer tokenizerAdjacentNodesFrom = new StringTokenizer(adjacentNodesFromDeg, DELIM_COMMA);
				int noOfAdjacentNodesFrom = tokenizerAdjacentNodesFrom.countTokens();
				String[] adjacentTokensFrom = new String[noOfAdjacentNodesFrom];

				// Pull out toDeg. nodes
				StringTokenizer tokenizer4 = new StringTokenizer(toDegreeAdjNodes, DELIM_TILDE);
				String adjacentNodesToDeg = tokenizer4.nextToken();
				int distanceTo = Integer.parseInt(tokenizer4.nextToken());
				IntWritable distanceValTo = new IntWritable(distanceTo);
				StringTokenizer tokenizerAdjacentNodesTo = new StringTokenizer(adjacentNodesToDeg, DELIM_COMMA);
				int noOfAdjacentNodesTo = tokenizerAdjacentNodesTo.countTokens();
				String[] adjacentTokensTo = new String[noOfAdjacentNodesTo];

				Text word = new Text();
				int counter1 = 0;
				// First emit all node pairs for the fromDegree
				while (tokenizerAdjacentNodesFrom.hasMoreTokens()) {
					String adjacentNodeFrom = tokenizerAdjacentNodesFrom.nextToken();
					adjacentTokensFrom[counter1++] = adjacentNodeFrom;
					word.set(nodeMain + DELIM_COLON + adjacentNodeFrom);
					context.write(word, distanceValFrom);
				}

				counter1 = 0;
				// Next (if relevant) emit all node pairs for the toDegree
				while (tokenizerAdjacentNodesTo.hasMoreTokens()) {
					String adjacentNodeTo = tokenizerAdjacentNodesTo.nextToken();
					adjacentTokensTo[counter1++] = adjacentNodeTo;
					word.set(nodeMain + DELIM_COLON + adjacentNodeTo);
					if (fromDegree != toDegree) {
						context.write(word, distanceValTo);
					}
				}

				int counterFrom, counterTo;
				// Now emit all possible node pairs between fromNode & toNode
				IntWritable distanceValTotal = new IntWritable((distanceFrom + distanceTo));
				for (counterFrom = 0; counterFrom < noOfAdjacentNodesFrom; counterFrom++) {
					for (counterTo = 0; counterTo < noOfAdjacentNodesTo; counterTo++) {
						// TODO: remove, check might be redundant!
						if (!adjacentTokensFrom[counterFrom].trim().equals(adjacentTokensTo[counterTo].trim())) {
							word.set(adjacentTokensFrom[counterFrom] + DELIM_COLON + adjacentTokensTo[counterTo]);
							context.write(word, distanceValTotal);
							word.set(adjacentTokensTo[counterTo] + DELIM_COLON + adjacentTokensFrom[counterFrom]);
							context.write(word, distanceValTotal);
						}
					}
				}
			} else {
				System.out.println("Nothing to do fromDegree/ toDegree adjacentNodes empty");
			}
		}


		/**
		 * Input values are in the form: <Node>:<Node>,..,<Node>~Distance| 
		 * E.g.: 	1:2,3~1| => distance between nodes (1,2) & (1,3) is 1. 
		 * 			1:8~2|9~4| => distance between nodes (1,8) is 2 & (1,9) is 4.
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			long currentRunVal = context.getConfiguration().getLong(CURRENT_RUN_DEGREE_SEP, 0);
			int fromDegree, toDegree;

			// TODO: messy, won't score!
			if (currentRunVal >= 2) {
				// 2 = 1+1
				fromDegree = 1;
				toDegree = 1;
				emitAllNodePairs(key, value, context, fromDegree, toDegree);
			}
			if (currentRunVal >= 3) {
				// 3 = 1+2
				fromDegree = 1;
				toDegree = 2;
				emitAllNodePairs(key, value, context, fromDegree, toDegree);
			}
			if (currentRunVal >= 4) {
				// 4 = 1+3
				fromDegree = 1;
				toDegree = 3;
				emitAllNodePairs(key, value, context, fromDegree, toDegree);

				// 4 = 2+2
				fromDegree = 2;
				toDegree = 2;
				emitAllNodePairs(key, value, context, fromDegree, toDegree);
			}
			if (currentRunVal >= 5) {
				// 5 = 1+4
				fromDegree = 1;
				toDegree = 4;
				emitAllNodePairs(key, value, context, fromDegree, toDegree);

				// 5 = 2+3
				fromDegree = 2;
				toDegree = 3;
				emitAllNodePairs(key, value, context, fromDegree, toDegree);
			}
		}
	}
	
	
	/** ------------Common Output Generator Mappers------------- **/

	/**
	 * Common Mapper 1: To recreate output similar to the original input file 
	 * 	Input values are in the form: <Node>:<Node><TAB>Distance 
	 * 	Output value:(<Node>,<Node>) as (K,V) ignoring the distance
	 * 
	 * @author algo
	 */
	public static class GraphOutputGeneratorMap extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer1 = new StringTokenizer(line, DELIM_TAB);
			StringTokenizer nodeTokens = new StringTokenizer(tokenizer1.nextToken(), DELIM_COLON);
			// Write the two nodes - ignoring the distance
			context.write(new Text(nodeTokens.nextToken()), new Text(nodeTokens.nextToken()));
		}
	}

	/**
	 * Common Mapper 2: To merge all input files each having Input data in the form:
	 * <Node>:<Node>,..<NODE>~Distance|
	 * 
	 * The data for any given node is split across rows/ across files aim is to
	 * combine them into one row
	 * 
	 * @author algo
	 */
	public static class GraphInputFilesMergeMap extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			// Main node
			StringTokenizer tokenizer1 = new StringTokenizer(line, DELIM_COLON);
			String nodeMain = tokenizer1.nextToken();

			// Associated nodes
			StringTokenizer tokenizer2 = new StringTokenizer(tokenizer1.nextToken(), DELIM_LINE);
			while (tokenizer2.hasMoreTokens()) {
				context.write(new Text(nodeMain), new Text(tokenizer2.nextToken()));
			}
		}
	}
}
