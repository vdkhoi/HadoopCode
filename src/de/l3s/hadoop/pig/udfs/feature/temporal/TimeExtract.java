package de.l3s.hadoop.pig.udfs.feature.temporal;
import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;


public class TimeExtract extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {
		String str = null;
		String granularity = null;
		if (input == null || input.size() == 0 || input.size() > 2)
			return null;
		try {
			str = (String)input.get(0);
			if (str == null) return null;
			granularity = (String)input.get(1);
			
			if (granularity.toLowerCase().equals("day")) {
				return str.substring(0, 10);
			}
			
			if (granularity.toLowerCase().equals("month")) {
				return str.substring(0, 7);
			}
			
			if (granularity.toLowerCase().equals("year")) {
				return str.substring(0, 4);
			}
			
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row " + str, e);
		}
		return null;
	}
}
