package de.l3s.hadoop.pig.udfs;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class Convert2ArchiveURL extends EvalFunc<String> {
	
	public static String getDest_url_archived(String dest_url ) {
		String decodeURL = dest_url;    	
    	String first = "", later = "", revfirst = "";
    	int split_pos = decodeURL.indexOf('/');
    	
    	if (split_pos < 0) { 
    		split_pos = decodeURL.length();
    	}
    	first = decodeURL.substring(0, split_pos);
    	
    	if (split_pos < decodeURL.length())
    		later = decodeURL.substring(split_pos + 1);
    	
    	
    	if ( split_pos >= 4){
    		
    		String[] dn = first.split(Pattern.quote("."));
    		int len = dn.length;
    		revfirst = dn[0];
    		for (int i = 1; i < len; i++) {
    			revfirst = dn[i] + "," + revfirst;
    		}
    		
    	}
        return (revfirst + ")/" + later).toLowerCase();
    }
	
	public String exec(Tuple input) throws IOException {
		String str = null;
	
		if (input == null || input.size() == 0)
			return null;
		try {
			str = (String) input.get(0);
			
			
			if (str.indexOf("http://www.") == 0)
				str = str.replaceFirst(
						Pattern.quote("http://www."), "");

			if (str.indexOf("https://www.") == 0)
				str = str.replaceFirst(
						Pattern.quote("https://www."), "");

			if (str.indexOf("http://") == 0)
				str = str.replaceFirst(
						Pattern.quote("http://"), "");

			if (str.indexOf("https://") == 0)
				str = str.replaceFirst(
						Pattern.quote("https://"), "");
			
			if (str.indexOf("www.") == 0)
			str = str.replaceFirst(
					Pattern.quote("www."), ""); 
			
			str = getDest_url_archived(str);
			progress();
			return str;

		} catch (Exception e) {
			throw new IOException("Caught exception processing input row " + str, e);
		}
	}
}
