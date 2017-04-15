package de.l3s.hadoop.pig.udfs;
import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;


public class ExtractDomain extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {
		String str = "";
		String value = "";
		if (input == null || input.size() == 0)
			return null;
		try {
			str = (String) input.get(0);
			if (str == null) return null;
			if (str.length() < 4) return str;
			int idx = str.indexOf(")");
			value = str;
			if (idx > 0)
				value = str.substring(0, idx);
			progress();
			return value;
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row " + str, e);
		}
	}
}
