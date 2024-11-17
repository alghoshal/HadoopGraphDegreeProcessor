package in.inkind.degsep.mr;

import java.io.IOException;

import junit.framework.TestCase;

public class BaseMRTest extends TestCase {

	public void testEmitAllNodePairsFrom1To2Deg() throws IOException, InterruptedException {
		assertTrue(BaseMR.isEmpty(null));
		assertTrue(BaseMR.isEmpty("   "));
		assertFalse(BaseMR.isEmpty("abdsd"));
		assertFalse(BaseMR.isEmpty(BaseMR.DELIM_COLON));
	}
	


}
