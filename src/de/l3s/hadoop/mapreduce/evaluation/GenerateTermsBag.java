package de.l3s.hadoop.mapreduce.evaluation;

import java.io.*;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class GenerateTermsBag extends Configured implements Tool {

	private static class Map extends
			Mapper<LongWritable, Text, Text, LongWritable> {

		private final static LongWritable ONE = new LongWritable(1);
		
		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value != null) {
					GermanAnalyzer analyzer = new GermanAnalyzer();
					String[] line = value.toString().split("\t");
					TokenStream tokens = analyzer.tokenStream(null, line[1]
							+ " " + line[2]);
					CharTermAttribute charTermAttrib = tokens
							.getAttribute(CharTermAttribute.class);
					tokens.reset();
					while (tokens.incrementToken()) {
						String term = charTermAttrib.toString();
						if (term.length() > 1 && !term.matches("^[-+]?\\d+(\\.\\d+)?$")) {
							output.write(new Text(term), ONE);
						}
						
					}
					tokens.end();
					tokens.close();
					analyzer.close();
				}
			} catch (IOException e) {
				throw new IOException(value.toString(), e);
			} catch (InterruptedException e1) {
				throw e1;
			} catch (Exception e2) {
				throw new IOException(value.toString(), e2);
			}
		}
	}

	private static class Combine extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Context output) throws IOException, InterruptedException {
			Iterator<LongWritable> iter = values.iterator();
			long sum = 0;
			while (iter.hasNext()) {
				sum += iter.next().get();
			}
			output.write(key, new LongWritable(sum));
		}
	}

	private static class Reduce extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Context output) throws IOException, InterruptedException {
			Iterator<LongWritable> iter = values.iterator();
			long sum = 0;
			while (iter.hasNext()) {
				sum += iter.next().get();
			}
			output.write(key, new LongWritable(sum));
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		conf.setBoolean("map.output.compress", true);
		conf.set("mapreduce.output.compression.type", "BLOCK");
		conf.setClass("mapreduce.output.fileoutputformat.compress.codec",
				org.apache.hadoop.io.compress.BZip2Codec.class,
				org.apache.hadoop.io.compress.CompressionCodec.class);
		conf.set("mapreduce.task.timeout", "1800000");
		conf.set("mapreduce.map.java.opts", "-Xmx3072m");
		conf.set("mapreduce.child.java.opts", "-Xmx3072m");

		Job job = Job.getInstance(conf, this.getClass().getName());

		job.setJobName(this.getClass().getName() + ".jar");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setJarByClass(GenerateTermsBag.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);
		

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job,
				org.apache.hadoop.io.compress.BZip2Codec.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new GenerateTermsBag(),
				args);
		System.exit(res);
	}
}