package de.l3s.hadoop.pig.udfs.feature.anchor;
import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;


public class CountQueryFreqInContent extends EvalFunc<Integer> {
	public Integer exec(Tuple input) throws IOException {
		String query = null;
		String content = null;
		if (input == null || input.size() != 2)
			return new Integer(0);
		try {
			if (input.get(0) == null || input.get(1) == null)
				return new Integer(0);
			query = input.get(0).toString();
			content = input.get(1).toString();
			content = content.toLowerCase();
			int current_pos = -1, count = 0;
			content = content.toLowerCase();
			query = query.toLowerCase().replace('_', ' ');
			progress();
			while (content.length() >= query.length()) {
				current_pos = content.indexOf(query);
				if (current_pos >= 0){
					count ++;
					content = content.substring(current_pos + query.length(), content.length());
				}
				else
					break;
				progress();
			}
			
			return new Integer(count);
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row " + content, e);
		}
	}
}
