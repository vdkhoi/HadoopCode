package de.l3s.hadoop.pig.udfs;
import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;


public class ExtractMainURL extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {
		String str = null;
		String value = null;
		if (input == null || input.size() == 0)
			return null;
		try {
			int len;
			value = str = (String) input.get(0);
			if (str == null) return "undefined";
			if (str.length() < 4) return "undefined";
			if ((len = str.indexOf('?')) > 4)
				value = str.substring(0, len);
			progress();
			return value;
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row " + str, e);
		}
	}
}
