package de.l3s.hadoop.pig.udfs;
import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.joda.time.*;

public class CountAnchorTimeSpan extends EvalFunc<Integer> {
	
	public Integer exec(Tuple input) throws IOException {
		String content = null;
		if (input == null || input.size() > 1)
			return new Integer(0);
		try {
			if (input.get(0) == null)
				return 0;
			content = input.get(0).toString();
			if (content.length() == 0) return 0;
			
			long duration = 0;
			DateTime d = null;
			Period p = null;
			
			DateTime BEGIN_DATE = DateTime.parse("1990-01-01"); 
			String[] series = content.trim().split(" ");
			PriorityQueue<Long> spanSeries = new PriorityQueue<Long>();
			for (int i = 0; i < series.length; i ++) {
				d = DateTime.parse(series[i].trim());
				duration = Days.daysBetween(BEGIN_DATE, d).getDays();
				spanSeries.add(duration);
			}
			progress();

			int timeSpanCount = 0;
			duration = spanSeries.peek();
			long span = 0;
			
			while (!spanSeries.isEmpty()) {
				span = spanSeries.poll();
				if (Math.abs(span - duration) > 7) {
					timeSpanCount ++;
				}
				duration = span;
			}
			
			return new Integer(timeSpanCount);
		} catch (Exception e) {
			throw new IOException("Caught exception processing input row ", e);
		}
	}
}
