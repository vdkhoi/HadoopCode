package de.l3s.hadoop.pig.udfs;
import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;


public class ExtractAnchors extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {
		String str = null;
		String[] values = null;
		int addr = 0;
		if (input == null || input.size() == 0)
			return null;
		try {
			addr = ((Long) input.get(0)).intValue();
			str = (String) input.get(1);
			if (str.length() < 4) return null;
			str = str.substring(2, str.length() - 2);
			progress();
			values = str.split("\\),\\(");
			progress();
			str = "";
			int i = 0, k = 0;		
			for (i = 0; i < values.length; i++) {
				progress();
				if (values[i] == null) {
					k ++;
					continue; 
				}
				if (addr > 0)
					values[i] = values[i].substring(addr + 1);
				progress();
				str += (values[i] + ((i == values.length - 1)? "" : " | "));
			}
			values = null;
			return str + "\t" + (i - k);
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row " + str, e);
		}
	}
}
