package de.l3s.hadoop.pig.udfs;
import java.io.IOException;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;


public class CompareAnchorAndEntity extends FilterFunc{
	private String str1 = null;
	private String str2 = null;
	
	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.size() != 2)
			return false;
		try {
			progress();
			if (input.get(0) == null || input.get(1) == null)
				return false;
			str1 = input.get(0).toString().toLowerCase().trim();
			str2 = input.get(1).toString().toLowerCase().trim();
			str2.replace('_', ' ');
			
			return str1.indexOf(str2) >= 0; 
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row " + str1 + str2, e);
		}
	}
}