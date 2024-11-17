package in.inkind.degsep.mr;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.Reducer;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import in.inkind.degsep.mr.GraphReducers.GraphDegreeSeparationIdentifierReduce;
import in.inkind.degsep.mr.GraphReducers.GraphInputFilesMergeReduce;
import in.inkind.degsep.mr.GraphReducers.GraphOutputGeneratorReduce;
import junit.framework.TestCase;
/**
 * Tests for the Reducers
 * 
 * @author algo
 */
public class GraphReducersTest extends TestCase {
	
	@Mock
	RawKeyValueIterator rawKeyValueIterator;
	
	@Mock
	Reducer.Context mockContext = Mockito.mock(Reducer.Context.class);
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		MockitoAnnotations.initMocks(this);
	}

	public void testDegreeSepIdReducerOneDegSep() throws IOException, InterruptedException {
		JobConf conf = new JobConf();
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setLong(BaseMR.CURRENT_RUN_DEGREE_SEP, 1);

		doNothing().when(mockContext).write(any(Text.class), any(IntWritable.class));
		when(mockContext.getConfiguration()).thenReturn(conf);
		
		ArgumentCaptor<Text> keyCaptor = ArgumentCaptor.forClass(Text.class);
		ArgumentCaptor<IntWritable> valueCaptor = ArgumentCaptor.forClass(IntWritable.class);
		
		Text inputKey = new Text("1:2");
		List<IntWritable> inputValues = Arrays.asList(new IntWritable(4),new IntWritable(1), new IntWritable(3));
		
		new GraphDegreeSeparationIdentifierReduce().reduce(inputKey, inputValues, mockContext);
		
		verify(mockContext,times(1)).write(any(Text.class), any(IntWritable.class));
		verify(mockContext,times(1)).write(keyCaptor.capture(), valueCaptor.capture());
		assertEquals(Arrays.asList(inputKey),keyCaptor.getAllValues());
		assertEquals(Arrays.asList(inputKey),keyCaptor.getAllValues());
		assertEquals(Arrays.asList(Collections.min(inputValues)), valueCaptor.getAllValues());
	}
	
	public void testDegreeSepIdReducerNoOutputForDegSeparationLowerToCurrRun() throws IOException, InterruptedException {
		JobConf conf = new JobConf();
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setLong(BaseMR.CURRENT_RUN_DEGREE_SEP, 2);
		
		doNothing().when(mockContext).write(any(Text.class), any(IntWritable.class));
		when(mockContext.getConfiguration()).thenReturn(conf);
		
		ArgumentCaptor<Text> keyCaptor = ArgumentCaptor.forClass(Text.class);
		ArgumentCaptor<IntWritable> valueCaptor = ArgumentCaptor.forClass(IntWritable.class);

		Text inputKey = new Text("1:2");
		new GraphDegreeSeparationIdentifierReduce().reduce(inputKey, 
				Arrays.asList(new IntWritable(4),new IntWritable(1), new IntWritable(3)), mockContext);
		
		// Nothing written
		verify(mockContext,never()).write(keyCaptor.capture(), valueCaptor.capture());
		assertTrue(keyCaptor.getAllValues().isEmpty());
		assertTrue(valueCaptor.getAllValues().isEmpty());

	}
	
	public void testOutputGeneratorReducerOneDegSep() throws IOException, InterruptedException {
		JobConf conf = new JobConf();
		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setLong(BaseMR.CURRENT_RUN_DEGREE_SEP, 1);
		
		doNothing().when(mockContext).write(any(Text.class), any(NullWritable.class));
		when(mockContext.getConfiguration()).thenReturn(conf);
		
		ArgumentCaptor<Text> keyCaptor = ArgumentCaptor.forClass(Text.class);
		ArgumentCaptor<NullWritable> valueCaptor = ArgumentCaptor.forClass(NullWritable.class);

		new GraphOutputGeneratorReduce().reduce(new Text("1"), 
				Arrays.asList(new Text("2"), new Text("3")), mockContext);
		
		Text outputKey = new Text("1:2,3~1|");
		
		verify(mockContext,times(1)).write(any(Text.class), any(NullWritable.class));
		verify(mockContext,times(1)).write(keyCaptor.capture(), valueCaptor.capture());
		assertEquals(Arrays.asList(outputKey),keyCaptor.getAllValues());
		assertEquals(Arrays.asList(NullWritable.get()),valueCaptor.getAllValues());
		
	}
	
	public void testInputFilesMergeReducerOneLevel() throws IOException, InterruptedException {
		JobConf conf = new JobConf();
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(NullWritable.class);
		conf.setLong(BaseMR.CURRENT_RUN_DEGREE_SEP, 1);
		
		doNothing().when(mockContext).write(anyObject(), anyObject());
		when(mockContext.getConfiguration()).thenReturn(conf);
		
		ArgumentCaptor<Text> keyCaptor = ArgumentCaptor.forClass(Text.class);
		ArgumentCaptor<NullWritable> valueCaptor = ArgumentCaptor.forClass(NullWritable.class);

		new GraphInputFilesMergeReduce().reduce(new Text("1"), Arrays.asList(new Text("2,3~1")), mockContext);
		
		Text outputKey = new Text("1:2,3~1|");
		verify(mockContext,times(1)).write(any(Text.class), any(NullWritable.class));
		verify(mockContext,times(1)).write(keyCaptor.capture(), valueCaptor.capture());
		assertEquals(Arrays.asList(outputKey),keyCaptor.getAllValues());
		assertEquals(Arrays.asList(NullWritable.get()),valueCaptor.getAllValues());
		
	}
	
	public void testInputFilesMergeReducerMultiLevel() throws IOException, InterruptedException {
		JobConf conf = new JobConf();
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(NullWritable.class);
		conf.setLong(BaseMR.CURRENT_RUN_DEGREE_SEP, 1);

		doNothing().when(mockContext).write(any(Text.class), any(NullWritable.class));
		when(mockContext.getConfiguration()).thenReturn(conf);
		
		ArgumentCaptor<Text> keyCaptor = ArgumentCaptor.forClass(Text.class);
		ArgumentCaptor<NullWritable> valueCaptor = ArgumentCaptor.forClass(NullWritable.class);

		new GraphInputFilesMergeReduce().reduce(new Text("1"), 
				Arrays.asList(new Text("2,3~1"), new Text("4~2")), mockContext);

		Text outputKey = new Text("1:2,3~1|4~2|");
		verify(mockContext,times(1)).write(any(Text.class), any(NullWritable.class));
		verify(mockContext,times(1)).write(keyCaptor.capture(), valueCaptor.capture());
		assertEquals(Arrays.asList(outputKey),keyCaptor.getAllValues());
		assertEquals(Arrays.asList(NullWritable.get()),valueCaptor.getAllValues());
	}
	
	public void testInputFilesMergeReducerMultiLevelUnsortedInputValues() throws IOException, InterruptedException {
		JobConf conf = new JobConf();
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(NullWritable.class);
		conf.setLong(BaseMR.CURRENT_RUN_DEGREE_SEP, 1);

		doNothing().when(mockContext).write(any(Text.class), any(NullWritable.class));
		when(mockContext.getConfiguration()).thenReturn(conf);
		
		ArgumentCaptor<Text> keyCaptor = ArgumentCaptor.forClass(Text.class);
		ArgumentCaptor<NullWritable> valueCaptor = ArgumentCaptor.forClass(NullWritable.class);

		new GraphInputFilesMergeReduce().reduce(new Text("1"), 
				Arrays.asList(new Text("4~2"), new Text("2,3~1")), mockContext);

		Text outputKey = new Text("1:2,3~1|4~2|");
		verify(mockContext,times(1)).write(any(Text.class), any(NullWritable.class));
		verify(mockContext,times(1)).write(keyCaptor.capture(), valueCaptor.capture());
		assertEquals(Arrays.asList(outputKey),keyCaptor.getAllValues());
		assertEquals(Arrays.asList(NullWritable.get()),valueCaptor.getAllValues());
	}
}
