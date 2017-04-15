package de.l3s.hadoop.mapreduce.feature;

import java.io.*;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import java.util.Iterator;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//	Format:
//	url TAB anchors TAB inlink

public class BuildDocumentLengthData extends Configured implements Tool {

	private static class Map extends
			Mapper<LongWritable, Text, Text, LongWritable> {
		private Text textKey = null;
		private LongWritable longValue = null;

		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value == null) return;
				String[] line = value.toString().split("\t");
				if (line.length == 4) {
					Tokenizer tokenizer = new StandardTokenizer(new StringReader(StringEscapeUtils.unescapeHtml(line[3].trim()).toLowerCase()));
					CharTermAttribute charTermAttrib = tokenizer
							.getAttribute(CharTermAttribute.class);
					tokenizer.reset();
					while (tokenizer.incrementToken()) {
						textKey = new Text(line[0].trim() + "\t" + line[1] + "\t" + line[2] + "\t" + charTermAttrib.toString());
						longValue = new LongWritable(1);
						output.write(textKey,  longValue);
					}
					tokenizer.end();
					tokenizer.close();
				}
			} catch (IOException e) {
				throw new IOException(value.toString(), e);
			}
			catch (InterruptedException e1){
				throw e1;
			}
			
			catch (Exception e2){
				throw new IOException(value.toString(), e2);
			}
		}
	}

	private static class Reduce extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Context output) throws IOException, InterruptedException {
			String[] line = key.toString().split("\t");
			Iterator<LongWritable> iter = values.iterator();
			long sum = 0;
			while (iter.hasNext()) {
				sum += iter.next().get();
			}
			output.write(new Text(line[2] + "\t" + line[3]), new LongWritable(sum));
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
		// conf.setInputFormat(TextInputFormat.class);
		// conf.setOutputFormat(TextOutputFormat.class);

		Job job = Job.getInstance(conf, "BuildTermFrequencyNoRevisionData");

		job.setJobName(this.getClass().getName() + ".jar");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setJarByClass(BuildDocumentLengthData.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job,
				org.apache.hadoop.io.compress.BZip2Codec.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new BuildDocumentLengthData(), args);
		System.exit(res);
	}
}