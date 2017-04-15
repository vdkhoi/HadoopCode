package de.l3s.hadoop.pig.udfs;
import java.io.IOException;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;


public class CheckURL extends FilterFunc{
	private String str = null;
	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		try {
			progress();
			str = (String) input.get(0);
			if (str == null) return false;
			
			if (str.length() < 6) return false;
			
			int i = str.indexOf(' ');
			if (i >= 0) return false;
			
			i = str.indexOf(")/");
			if (i < 0) return false;
			progress();
			int j = str.indexOf(",");
			progress();
			return j < i - 1; 
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row " + str, e);
		}
	}
}
