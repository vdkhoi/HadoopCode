package de.l3s.hadoop.pig.udfs;
import java.io.IOException;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;


public class CheckTime extends FilterFunc{
	private String strTime = null;
	private String strPart = null;
	private String value = null;
	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0 || input.size() > 3)
			return false;
		try {
			progress();
			strTime = (String) input.get(0);
			strPart = (String) input.get(1);
			value = (String) input.get(2);
			if (strTime == null) return false;
			if (strPart.toLowerCase().equals("year")) {
				if (strTime.substring(0, 4).equals(value))
					return true;
			}
			
			if (strPart.toLowerCase().equals("month")) {
				if (strTime.substring(5, 7).equals(value))
					return true;
			}
			
			if (strPart.toLowerCase().equals("day")) {
				if (strTime.substring(8, 10).equals(value))
					return true;
			}

			return false; 
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row " + strTime, e);
		}
	}
}
