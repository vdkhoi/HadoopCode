package de.l3s.hadoop.pig.udfs;
import java.io.IOException;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;


public class IsIntraDomain extends FilterFunc{
	private String link1 = null, link2 = null, str = null;;
	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		try {
			progress();
			link1 = (String) input.get(0);
			link2 = (String) input.get(1);
			if ((link1 == null) || (link2  == null)) return false;
			str = link1 + "\t" + link2;
			link1 = link1.substring(0, link1.indexOf(')')).toLowerCase();
			link2 = link2.substring(0, link2.indexOf(')')).toLowerCase();
			progress();
			if (link1.length() == link2.length()) {
				return link1.compareTo(link2) == 0;
			}
			else if (link1.length() > link2.length()) {
				return link1.indexOf(link2) >= 0;
			}
			else {
				return link2.indexOf(link1) >= 0;
			}
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row " + str, e);
		}
	}
}