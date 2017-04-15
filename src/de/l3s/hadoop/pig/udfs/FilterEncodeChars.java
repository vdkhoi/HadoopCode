package de.l3s.hadoop.pig.udfs;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.commons.lang.*;


public class FilterEncodeChars extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		try {
			progress();
			String str = StringEscapeUtils.unescapeHtml((String) input.get(0));
			progress();
			return str;
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
}
