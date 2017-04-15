package de.l3s.hadoop.mapreduce.graph;


import java.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

//Prepare datasets:
//step1: Fetch root set from Bing
//step2: Convert Bing result to SURT form
//step3: Build base set from map/reduce   (this program)

public class MergeArchiveURLTogether extends Configured implements Tool {

	private static class Map extends
			Mapper<LongWritable, Text, Text, NullWritable> {

		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value == null)
					return;
				String row = value.toString();
				if (row == null)
					return;
				String[] line = row.split("\t", 2);

				output.write(new Text(line[1]), NullWritable.get());
			} catch (Exception e) {
				throw new IOException(value.toString(), e);
			}
		}
	}

	private static class Reduce extends
			Reducer<Text, NullWritable, Text, NullWritable> {
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Context output) throws IOException, InterruptedException {
			
			output.write(key, NullWritable.get());
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		conf.setBoolean("map.output.compress", true);
		conf.set("mapreduce.output.compression.type", "BLOCK");
		
		//conf.setClass("mapreduce.output.fileoutputformat.compress.codec",
		//		org.apache.hadoop.io.compress.BZip2Codec.class,
		//		org.apache.hadoop.io.compress.CompressionCodec.class);
		
		conf.setClass("mapreduce.output.fileoutputformat.compress.codec",
				org.apache.hadoop.io.compress.GzipCodec.class,
				org.apache.hadoop.io.compress.CompressionCodec.class);
		
		conf.set("mapreduce.task.timeout", "1800000");
		conf.set("mapreduce.map.java.opts", "-Xmx3072m");
		conf.set("mapreduce.child.java.opts", "-Xmx3072m");

		Job job = Job.getInstance(conf, this.getClass().getName());

		job.setJobName(this.getClass().getName() + ".jar");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setJarByClass(MergeArchiveURLTogether.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job,
				org.apache.hadoop.io.compress.GzipCodec.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(),
				new MergeArchiveURLTogether(), args);
		System.exit(res);

	}
}