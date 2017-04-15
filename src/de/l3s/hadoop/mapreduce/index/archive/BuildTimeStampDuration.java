package de.l3s.hadoop.mapreduce.index.archive;

import java.io.*;
import java.util.Iterator;
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
//  src time dest anchor
//	url TAB timestamp TAB anchors TAB inlink

public class BuildTimeStampDuration extends Configured
		implements Tool {

	private static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text textKey = null;
		private Text textValue = null;

		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value == null)
					return;
				String[] line = value.toString().split("\t");
				if (line.length == 3) {
					textKey = new Text(line[0] + "\t" + line[1]);
					textValue = new Text(line[2]);
					output.write(textKey, textValue);
				}
			} catch (IOException e) {
				throw new IOException(value.toString(), e);
			} catch (InterruptedException e1) {
				throw e1;
			}

			catch (Exception e2) {
				throw new IOException(value.toString(), e2);
			}
		}
	}


	private static class Reduce extends
			Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context output) throws IOException, InterruptedException {
			Iterator<Text> iter = values.iterator();
			
			String first = "", last = "", current = "";
			if (iter.hasNext()) {
				first = last = current = iter.next().toString();
			}
			while (iter.hasNext()) {
				current = iter.next().toString();
				if (current.compareTo(first) < 0) {
					first = current;
				}
				if (current.compareTo(last) > 0) {
					last = current;
				}
			}
			output.write(key, new Text(first + "\t" + last));
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

		Job job = Job.getInstance(conf, "BuildTimeStampDuration");

		job.setJobName(this.getClass().getName() + ".jar");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setJarByClass(BuildTimeStampDuration.class);
		job.setMapperClass(Map.class);
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
				new BuildTimeStampDuration(), args);
		System.exit(res);
	}
}



