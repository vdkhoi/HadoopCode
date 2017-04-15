package de.l3s.hadoop.pig.udfs.feature.URL;
import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;


public class CheckEntityOnLastPartOfURL extends EvalFunc<Integer>{
	private String lastPartOfURL = "", entity = "";
	public Integer exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return 0;
		try {
			lastPartOfURL = (String) input.get(0);
			entity = (String) input.get(1);
			
			int i = lastPartOfURL.lastIndexOf("/");
			lastPartOfURL = lastPartOfURL.substring(i + 1, lastPartOfURL.length());
			String[] terms = entity.toLowerCase().split("[^a-z]+");
			int lastOrder = -1, temp;
			int count = 0;
			for (int k = 0; k < terms.length; k ++) {
				if ((temp = lastPartOfURL.indexOf(terms[k])) >= lastOrder ) {
					lastOrder = temp;
					count ++;
				}
			}
			
			if (count == terms.length) 
				return 2;   // All terms are in last part of URL
			
			if (count < terms.length && count > 0)
				return 1;  // some terms are not in last part of URL
				
			if (count == 0)
				return 0;
			
			
			return 0; 
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row " + lastPartOfURL, e);
		}
	}
}
