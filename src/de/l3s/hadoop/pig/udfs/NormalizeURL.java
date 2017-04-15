package de.l3s.hadoop.pig.udfs;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class NormalizeURL extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {
		String str = null;
		String[] values = null;
		String rest = null;
		if (input == null || input.size() == 0)
			return null;
		try {
			str = (String) input.get(0);
			int i = -1;
			str = str.replace(")", "");
			i = str.indexOf("/");
			progress();
			rest = str.substring(i + 1, str.length());
			str = str.substring(0, i);
			values = str.split(",");
			str = "";
			progress();
			for (i = values.length - 1; i > 0; i--)
				str = str + values[i] + ".";
			
			progress();
			str = str + values[0] + "/" + rest;
			values =null;
			rest = null;
			return str;

		} catch (Exception e) {
			throw new IOException("Caught exception processing input row " + str, e);
		}
	}
}
