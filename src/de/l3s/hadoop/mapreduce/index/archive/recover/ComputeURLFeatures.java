package de.l3s.hadoop.mapreduce.index.archive.recover;

// Feature 3, 5, 15

import java.io.*;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
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

public class ComputeURLFeatures extends Configured implements Tool {
	public static final int MAX_LENGTH = 10000;

	private static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text textKey = null;
		private Text textValue = null;

		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value != null) {
					String[] line = value.toString().split("\t", 2);

					if (line.length == 2 && line[0].length() > 0) {
						textKey = new Text(line[0].trim());
						textValue = new Text(
								StringEscapeUtils.unescapeHtml(line[1].trim()));
						output.write(textKey, textValue);
					}
				}
			} catch (IOException e) {
				throw new IOException("Error here: " + value.toString(), e);
			} catch (InterruptedException e1) {
				throw new InterruptedException("Error here: " + value.toString() + "\r\n" + e1);
			}

			catch (Exception e2) {
				throw new IOException("Error here: " + value.toString(), e2);
			}
		}
	}
	
	

	private static class Reduce extends Reducer<Text, Text, Text, Text> {

		public int feature5_cal(String url) {
			if (StringUtils.contains("suche", url)) {
				if (StringUtils.contains("suchergebnis", url))
					return 1;
				if (StringUtils.contains("suchen", url))
					return 1;
				return 1;
			}
			if (StringUtils.contains("query", url))
				return 1;
			if (StringUtils.contains("suchwort", url))
				return 1;
			return 0;
		}
		
		public int feature15_URLDepth(String url) {
			return StringUtils.countMatches("/", url);
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context output)
				throws IOException, InterruptedException {
			try {
				String url = key.toString(); 
				String val = values.iterator().next().toString();
				int feature5_searchword = feature5_cal(url);
				int feature15_urldepth = feature15_URLDepth(url);
				output.write(key, new Text(val + "\t" + feature5_searchword + "\t" + feature15_urldepth) );
			} catch (IOException e) {
				throw new IOException(key.toString(), e);
			} catch (InterruptedException e1) {
				throw new InterruptedException(key.toString().toString()
						+ "\r\n" + e1);
			} catch (Exception e2) {
				throw new IOException(key.toString(), e2);
			}
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

		Job job = Job.getInstance(conf, this.getClass().getName());

		job.setJobName(this.getClass().getName() + ".jar");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setJarByClass(ComputeURLFeatures.class);
		job.setMapperClass(Map.class);
		//job.setCombinerClass(Reduce.class);
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
				new ComputeURLFeatures(), args);
		System.exit(res);
	}
}