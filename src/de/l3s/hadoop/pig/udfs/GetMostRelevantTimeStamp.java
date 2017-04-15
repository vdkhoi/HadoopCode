package de.l3s.hadoop.pig.udfs;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;


public class GetMostRelevantTimeStamp extends EvalFunc<String>{
	private String str1 = null;
	private String str2 = null;
	
	public String exec(Tuple input) throws IOException {
		if (input == null)
			return null;
		if (input.size() != 2)
			return null;
		try {
			progress();
			
			if (input.get(0) == null) return null;
			if (input.get(1) == null) return null;
			
			str1 = (String) input.get(0);
			str2 = (String) input.get(1);
			
			if (str1.length() == 0) return null;
			if (str2.length() == 0) return null;
			
			str2 = str2.substring(0, str2.length() - 1);
			int i = 0;
			
			progress();
			
			String curr = "";
			
			int count = 0;
			
			while (str2.length() > 10) {
				i = str2.indexOf('|');
				if (i >= 0) {
					curr = str2.substring(0, i - 1);
					if (str1.compareTo(curr) > 0) break;
					if (i + 1 < str2.length()) {
						str2 = str2.substring(i + 1, str2.length());
					}
					count ++;
					if (count % 1000 == 0) {
						progress();
					}
					
				}
				else{
					break;
				}
					
			}
			
			if (curr.length() == 0) {
				curr = str2;
			}
			
			progress();
			return curr;
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row " + str1 + str2, e);
		}
	}
}