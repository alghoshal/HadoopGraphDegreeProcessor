package in.inkind.degsep.mr;

/**
 * Utilities
 * 
 * @author algo 
 */
public class BaseMR {

	public static final String DELIM_COLON=":";
	public static final String DELIM_COMMA=",";
	public static final String DELIM_TILDE="~";
	public static final String DELIM_LINE="|";
	public static final String DELIM_TAB ="\t";
	public static final String CURRENT_RUN_DEGREE_SEP = "currentRun";
	public static final int MAX_DEG_SEPARATION =6;
	
	
	/**------------Utilities-------------**/
	public static boolean isEmpty(String s){ return null==s || "".equals(s.trim()); }

}
