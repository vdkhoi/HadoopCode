package de.l3s.hadoop.pig.udfs.feature.URL;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class CalculateURLDepth extends EvalFunc<Integer> {
	
	public static Integer getURLDepth(String dest_url ) {
		int i = 0, current = -1 ;
		
		while (dest_url.length() > 0 && (current = dest_url.indexOf('/')) >= 0) {
			i++;
			dest_url = dest_url.substring(current + 1);
		}
        return i;
    }
	
	public Integer exec(Tuple input) throws IOException {
		String str = null;
	
		if (input == null || input.size() == 0)
			return null;
		try {
			str = (String) input.get(0);
			int count = 0;
			if (str.length() > 0) {
				count = getURLDepth(str);
			}
			progress();
			return count;

		} catch (Exception e) {
			throw new IOException("Caught exception processing input row " + str, e);
		}
	}
}
