package de.l3s.hadoop.pig.udfs;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;


public class CompareURL extends FilterFunc{
	private String str1 = null;
	private String str2 = null;
	
	public static String getDest_url_archived(String dest_url ) {
    	String decodeURL = dest_url;    	
    	String first = "", later = "", revfirst = "";
    	int split_pos = decodeURL.indexOf('/');
    	if ( split_pos >= 4){
    		first = decodeURL.substring(0, split_pos);
    		String[] dn = first.split(Pattern.quote("."));
    		int len = dn.length;
    		revfirst = dn[0];
    		for (int i = 1; i < len; i++) {
    			revfirst = dn[i] + "," + revfirst;
    		}
    		later = decodeURL.substring(split_pos + 1);
    	}
        return (revfirst + ")/" + later).toLowerCase();
    }
	
	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		try {
			progress();
			str1 = (String) input.get(0);
			str2 = (String) input.get(1);
			if (str1 != null && str2 != null )
				return str1.equals(getDest_url_archived(str2));
			progress();
			return false; 
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row " + str1 + str2, e);
		}
	}
}