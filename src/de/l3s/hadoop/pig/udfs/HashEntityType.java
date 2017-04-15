package de.l3s.hadoop.pig.udfs;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class HashEntityType extends EvalFunc<Integer> {
	

	
	public Integer exec(Tuple input) throws IOException {
		String str = null;
	
		if (input == null || input.size() == 0)
			return 0;
		try {
			str = (String) input.get(0);
			if (str.length() > 0) {
				return str.hashCode();
			}
			else {
				return 0;
			}
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row " + str, e);
		}
	}
}
