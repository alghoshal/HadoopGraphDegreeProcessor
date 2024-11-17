package in.inkind.degsep.mr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * All Mappers
 * 
 * @author algo 
 */
public class GraphReducers extends BaseMR {
	
	  /**------------Degree separation Map/Reduce-------------**/  

	 /**
	  * First Reducer: To find the Node pairs for a given deg. of separation between them.
	  * Outputs a new Node pair & the shortest distance between them
	  * 
	  * @author algo
	  */
	 public static class GraphDegreeSeparationIdentifierReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	      throws IOException, InterruptedException {
	    	long currentRunVal = context.getConfiguration().getLong(CURRENT_RUN_DEGREE_SEP, 0);
	        int min = Integer.MAX_VALUE;
	        for (IntWritable val : values) {
	            if(val.get()<min) min = val.get();
	        }
	        if(min>=currentRunVal){
	        	context.write(key, new IntWritable(min));
	        }
	    }
	 }
	 
	 /**------------Common Output Generator Reducers-------------**/
	 
	 /**
	  * Common Reducer 1: To recreate output similar to the original input file,
	  * upto one Distance level for a given key (node), i.e. only one DELIM_LINE section.
	  * Output is of the form (similar to the original input file format): 
	  * 	<Node>:<Node>,..,<Node>~Distance|
	  * 
	  * @author algo
	  */
	 public static class GraphOutputGeneratorReduce extends Reducer<Text, Text, Text, NullWritable> {
		
	    public void reduce(Text key, Iterable<Text> values, Context context) 
	      throws IOException, InterruptedException {
	    	long currentRunVal = context.getConfiguration().getLong(CURRENT_RUN_DEGREE_SEP, 0);
	        StringBuilder builder = new StringBuilder();
	        builder.append(key);
	        builder.append(DELIM_COLON);
	        for (Text val : values) {
	           builder.append(val.toString());
	           builder.append(DELIM_COMMA);
	        }
	        builder.replace(builder.length()-1, builder.length(), DELIM_TILDE);
	        builder.append(currentRunVal);
	        builder.append(DELIM_LINE);
	       	context.write(new Text(builder.toString()), NullWritable.get());
	    }
	 }
	 
	 /**
	  * Common Reducer 2: To recreate output similar to the original input file,
	  * all Distances levels for a given key (node), i.e. n-distance levels n- DELIM_LINE sections.
	  * 
	  * Output is of the form (similar to the original input file format): 
	  * 	<Node>:<Node>,..,<Node>~Distance|<Node>,..,<Node>~Distance|...|<Node>,..,<Node>~Distance
	  * 
 	  * Order maintained among different degree level results, 
 	  * i.e. 2nd-degree before 3rd-degree before 4th-degree.. & so on 
	  * 
	  * @author algo
	  */
	 public static class GraphInputFilesMergeReduce extends Reducer<Text, Text, Text, NullWritable> {
		
	    public void reduce(Text key, Iterable<Text> values, Context context) 
	      throws IOException, InterruptedException {
	    	String [] associatedDegNodes = new String[MAX_DEG_SEPARATION];
	    	//TODO: Custom shuffle/ sort
	    	for (Text val : values) {
	            StringTokenizer token = new StringTokenizer(val.toString(), DELIM_TILDE);
	            token.nextToken();
	            int degSep = Integer.parseInt(token.nextToken());
	            associatedDegNodes[degSep] = val.toString();
	        }
	    	
	    	StringBuilder builder = new StringBuilder();
	        builder.append(key);
	        builder.append(DELIM_COLON);
	        int i;
	        for(i=0;i<MAX_DEG_SEPARATION;i++){
	        	if(!isEmpty(associatedDegNodes[i])){
			        builder.append(associatedDegNodes[i]);
			        builder.append(DELIM_LINE);
	        	}
	        }
	       	context.write(new Text(builder.toString()), NullWritable.get());
	    }
	 }
}
