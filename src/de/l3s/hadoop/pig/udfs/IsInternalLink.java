package de.l3s.hadoop.pig.udfs;
import java.io.IOException;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;


public class IsInternalLink extends FilterFunc{
	private String str = null;
	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return false;
		try {
			progress();
			str = (String) input.get(0);
			if (str == null) return false;
			int j = str.substring(0, 2).compareTo("de");
			progress();
			return j == 0;
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row " + str, e);
		}
	}
}