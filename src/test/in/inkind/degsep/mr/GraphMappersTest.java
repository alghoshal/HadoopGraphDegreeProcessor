package in.inkind.degsep.mr;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import in.inkind.degsep.mr.GraphMappers.GraphDegreeSeparationIdentifierMap;
import in.inkind.degsep.mr.GraphMappers.GraphInputFilesMergeMap;
import in.inkind.degsep.mr.GraphMappers.GraphOutputGeneratorMap;
import junit.framework.TestCase;

/**
 * Tests for the Mappers
 * 
 * @author algo
 */
public class GraphMappersTest extends TestCase {

	@Mock
	Mapper.Context mockContext;
	
	public Map<String, List<String>> multiValuesWritten;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		MockitoAnnotations.initMocks(this);
		multiValuesWritten = new HashMap<String, List<String>>();
	}
	
	public void testEmitAllNodePairsFrom1To2Deg() throws IOException, InterruptedException {
		doAnswer(new CaptureArgumentsWrittenAsStrings<Void>()).when(mockContext).write(any(Text.class), any(IntWritable.class));
		
		Text inputValue = new Text("1:2,3~1|4~2|");
		new GraphDegreeSeparationIdentifierMap().emitAllNodePairs(null, inputValue, mockContext, 1, 2);
		
		verify(mockContext,times(7)).write(any(Text.class), any(IntWritable.class));
		assertEquals("1", multiValuesWritten.get("1:2").get(0));
		assertEquals("1", multiValuesWritten.get("1:3").get(0));
		assertEquals("2", multiValuesWritten.get("1:4").get(0));
		assertEquals("3", multiValuesWritten.get("2:4").get(0));
		assertEquals("3", multiValuesWritten.get("4:2").get(0));
		assertEquals("3", multiValuesWritten.get("3:4").get(0));
		assertEquals("3", multiValuesWritten.get("4:3").get(0));
	}

	public void testEmitAllNodePairsFor1To1Deg() throws IOException, InterruptedException {
		doAnswer(new CaptureArgumentsWrittenAsStrings<Void>()).when(mockContext).write(any(Text.class), any(IntWritable.class));
		
		Text inputValue = new Text("1:2,3~1|4~2|");
		new GraphDegreeSeparationIdentifierMap().emitAllNodePairs(null, inputValue, mockContext, 1, 1);

		verify(mockContext,times(6)).write(any(Text.class), any(IntWritable.class));
		assertNull(multiValuesWritten.get("1:4"));
		assertNull(multiValuesWritten.get("2:4"));
		assertNull(multiValuesWritten.get("3:4"));
	}

	public void testEmitAllNodePairsOnly1Deg() throws IOException, InterruptedException {
		doAnswer(new CaptureArgumentsWrittenAsStrings<Void>()).when(mockContext).write(any(Text.class), any(IntWritable.class));
		
		ArgumentCaptor<Text> keyCaptor = ArgumentCaptor.forClass(Text.class);
		ArgumentCaptor<IntWritable> valueCaptor = ArgumentCaptor.forClass(IntWritable.class);
		
		Text inputValue = new Text("1:2,3~1|");
		new GraphDegreeSeparationIdentifierMap().emitAllNodePairs(null, inputValue, mockContext, 1, 1);
		verify(mockContext,times(6)).write(any(Text.class), any(IntWritable.class));
		assertEquals("1", multiValuesWritten.get("1:2").get(0));
		assertEquals("1", multiValuesWritten.get("1:3").get(0));
		assertEquals("2", multiValuesWritten.get("2:3").get(0));
		assertEquals("2", multiValuesWritten.get("3:2").get(0));
	}

	public void testGraphOutputGeneratorMap() throws IOException, InterruptedException {
		doAnswer(new CaptureArgumentsWrittenAsStrings<Void>()).when(mockContext).write(any(Text.class), any(IntWritable.class));
		
		Text inputValue = new Text("1:2" + BaseMR.DELIM_TAB + "3");
		new GraphOutputGeneratorMap().map(null, inputValue, mockContext);
		verify(mockContext,times(1)).write(any(Text.class), any(IntWritable.class));
		assertEquals("2", multiValuesWritten.get("1").get(0));
	}

	public void testGraphInputFilesMergeMap() throws IOException, InterruptedException {
		doAnswer(new CaptureArgumentsWrittenAsStrings<Void>()).when(mockContext).write(any(Text.class), any(IntWritable.class));

		Text inputValue = new Text("1:2,3~1|4~2|");
		new GraphInputFilesMergeMap().map(null, inputValue, mockContext);

		verify(mockContext,times(2)).write(any(Text.class), any(IntWritable.class));
		assertEquals(1,multiValuesWritten.keySet().size());
		assertEquals(2, multiValuesWritten.get("1").size());
		assertEquals("2,3~1", multiValuesWritten.get("1").get(0));
		assertEquals("4~2", multiValuesWritten.get("1").get(1));
	}
	
	/**
	 * Captures Arguments to the Mapper.Context.write() method: 
	 * - Multiple calls to write() happen from the same caller
	 * - Along with reuse of mutable arguments objects by the caller, 
	 * 		so the equivalent immutable String value of the Argument is captured
	 * 
	 * Note: Due to reuse of a set of mutable argument objects by the caller, 
	 * Mockito.ArgumentCaptor (which uses argument object references) doesn't work.
	 * 
	 * @param <Void>
	 */
	public class CaptureArgumentsWrittenAsStrings<Void> implements Answer<Void>{
		public Void answer(InvocationOnMock invocation) {
			Object[] args = invocation.getArguments();
			System.out.println( "called with arguments: " + Arrays.toString(args));
			if (!multiValuesWritten.containsKey(args[0].toString())) multiValuesWritten.put(args[0].toString(), new ArrayList<String>());
			multiValuesWritten.get(args[0].toString()).add(args[1].toString());
			return null ;
		}
	}
}
