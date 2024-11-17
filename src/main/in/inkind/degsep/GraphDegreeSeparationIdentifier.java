package in.inkind.degsep;
        
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import in.inkind.degsep.mr.BaseMR;
import in.inkind.degsep.mr.GraphMappers.GraphDegreeSeparationIdentifierMap;
import in.inkind.degsep.mr.GraphMappers.GraphInputFilesMergeMap;
import in.inkind.degsep.mr.GraphMappers.GraphOutputGeneratorMap;
import in.inkind.degsep.mr.GraphReducers.GraphDegreeSeparationIdentifierReduce;
import in.inkind.degsep.mr.GraphReducers.GraphInputFilesMergeReduce;
import in.inkind.degsep.mr.GraphReducers.GraphOutputGeneratorReduce;
 
/**
 * Driver
 * 
 * @author algo 
 */
public class GraphDegreeSeparationIdentifier {
 
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Path inPath = new Path(args[0]);
    StringBuilder allInputPaths = new StringBuilder(); 
    allInputPaths.append(args[0]);
    Path outPath =  null;
    int iterations = 4, i;
    Job job;
    for (i = 0; i<iterations; ++i){
    	int currentRun = i+2;
    	
    	// First Job: Identify Node pairs having 2-deg separation   	
    	job = new Job(conf, "graphdegree"+i);
        
        job.setJarByClass(GraphDegreeSeparationIdentifier.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.getConfiguration().setLong(BaseMR.CURRENT_RUN_DEGREE_SEP, currentRun);
                 
        job.setMapperClass(GraphDegreeSeparationIdentifierMap.class);
        job.setReducerClass(GraphDegreeSeparationIdentifierReduce.class);
            
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
    	outPath = new Path(args[1]+i);
    	//FileInputFormat.addInputPaths(job, allInputPaths.toString());
    	FileInputFormat.addInputPath(job, inPath);
    	FileOutputFormat.setOutputPath(job, outPath);
    	job.waitForCompletion(true);
    	inPath = outPath;
    	
    	// Second Job: Format Output from 2-deg separation job
    	job = new Job(conf, "graphdegree-recreateoutput"+i);
        
        job.setJarByClass(GraphDegreeSeparationIdentifier.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.getConfiguration().setLong(BaseMR.CURRENT_RUN_DEGREE_SEP, currentRun);
         
        job.setMapperClass(GraphOutputGeneratorMap.class);
        job.setReducerClass(GraphOutputGeneratorReduce.class);
            
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        String pathAsString =args[1]+i+"/"+i ;
    	outPath = new Path(pathAsString);
    	FileInputFormat.addInputPath(job, inPath);
    	FileOutputFormat.setOutputPath(job, outPath);
    	job.waitForCompletion(true);
    	inPath = outPath;
    	allInputPaths.append(BaseMR.DELIM_COMMA);
    	allInputPaths.append(pathAsString);
    	
    	// Third Job: Merge diff. input files, 1-deg & 2-deg separation
    	job = new Job(conf, "graphdegree-merge"+i);
        
        job.setJarByClass(GraphDegreeSeparationIdentifier.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.getConfiguration().setLong(BaseMR.CURRENT_RUN_DEGREE_SEP, currentRun);
         
        job.setMapperClass(GraphInputFilesMergeMap.class);
        job.setReducerClass(GraphInputFilesMergeReduce.class);
            
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        pathAsString =args[1]+i+"/"+i+"/"+i ;
    	outPath = new Path(pathAsString);
    	FileInputFormat.addInputPaths(job, allInputPaths.toString());
    	FileOutputFormat.setOutputPath(job, outPath);
    	job.waitForCompletion(true);
    	inPath = outPath;
    	allInputPaths = new StringBuilder();
    	allInputPaths.append(pathAsString);
    }  
 }        
}
