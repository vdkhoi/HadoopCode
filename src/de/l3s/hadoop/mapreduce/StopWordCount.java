package de.l3s.hadoop.mapreduce;

import java.io.*;
import java.util.*;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class StopWordCount extends Configured implements Tool {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		static enum Counters {
			INPUT_WORDS
		}

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		private long numRecords = 0;
		private String inputFile;

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = StringEscapeUtils.unescapeHtml( value.toString().toLowerCase() );

			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
				reporter.incrCounter(Counters.INPUT_WORDS, 1);
			}

			if ((++numRecords % 100) == 0) {
				reporter.setStatus("Finished processing " + numRecords
						+ " records " + "from the input file: " + inputFile);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), StopWordCount.class);
		conf.setJobName(this.getClass().getName());

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new StopWordCount(), args);
		System.exit(res);
	}
}