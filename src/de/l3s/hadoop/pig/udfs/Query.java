package de.l3s.hadoop.pig.udfs;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;
import org.apache.commons.lang.*;


public class Query extends FilterFunc {
	public Boolean exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0  || input.size() > 3)
			return false;
		try {
			progress();
			String dest_url = (String) input.get(0);
			String anchors	= (String) input.get(1);
			String query_str = (String) input.get(2);
			if (dest_url == null || anchors == null) return false;
			anchors = StringEscapeUtils.unescapeHtml(anchors).toLowerCase();
			dest_url = dest_url.toLowerCase();
			query_str = query_str.toLowerCase(); 
			StringTokenizer token = new StringTokenizer(query_str, "_");
			String currentToken = null;
			boolean found = true;
			while (found && token.hasMoreTokens()) {
				currentToken = token.nextToken();
				found = found && ((dest_url.indexOf(currentToken) >= 0) || (anchors.indexOf(currentToken) >= 0));  
			}
			progress();
			return found;
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
}