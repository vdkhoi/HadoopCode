package de.l3s.hadoop.pig.udfs;
import java.io.IOException;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;


public class FilterURLByYearAndDomain extends FilterFunc{
	private String src = null;
	private Character src_val = null;
	private Character src_val1 = null;
	private String year = null;
	private String year_val = null;
	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.size() == 1 || input.size() == 2 || input.size() == 4 || input.size() > 5 )
			return false;
		try {
			src = (String) input.get(0);
			src_val = input.get(1).toString().charAt(0);
			src_val1 = input.get(2).toString().charAt(0);
			if (src == null) return false;
			
			progress();
			if (input.size() == 3) {
				return (src.charAt(3) == src_val.charValue()) || (src.charAt(3) == src_val1.charValue());
			}
			else {
				year = (String) input.get(3);
				year_val = (String) input.get(4);
				if (year == null) return false;
				progress();
				return (src.charAt(3) == src_val.charValue() || src.charAt(3) == src_val1.charValue()) && (year.substring(0, 4).equals(year_val));
			}
		} catch (Exception e) {
			throw new IOException(e.getMessage()+ "\r\nCaught exception FilterURLByYearAndDomain processing input row: " + input.toString() , e);
		}
	}
}