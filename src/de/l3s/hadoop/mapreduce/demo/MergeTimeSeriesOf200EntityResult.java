package de.l3s.hadoop.mapreduce.demo;

import java.io.*;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.hadoop.fs.Path;
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

public class MergeTimeSeriesOf200EntityResult extends Configured implements Tool {

	private static class DistinctMap extends Mapper<LongWritable, Text, Text, NullWritable> {

		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value != null) {
					String[] line = value.toString().split("\t");
					if (line.length == 4){
						output.write(new Text(line[0] + "\t" + line[1]), NullWritable.get());
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
	
	private static class MergeMap extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value != null) {
					String[] line = value.toString().split("\t");
					if (line.length == 2){
						output.write(new Text(line[0]), new Text(line[1]));
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
	
	private static int MAX_LENGTH = 10000;

	private static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context output)
				throws IOException, InterruptedException {
			try {
				String currentTime = "", timeSeriesStr = "";
				PriorityQueue<String> timeSeries = new PriorityQueue<String>();
				Iterator<Text> iter = values.iterator();
				while (iter.hasNext()) {
					currentTime = iter.next().toString();
					timeSeries.add(currentTime);
				}
				if (timeSeries.size() < MAX_LENGTH) {
					while (timeSeries.size() > 0) {
						timeSeriesStr += (timeSeries.poll() + "|");
						
					}
					output.write(key, new Text(timeSeriesStr));
				}
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

		Job job = Job.getInstance(conf, "MergeTimeSeriesAndCountOfAQuery");

		job.setJobName(this.getClass().getName() + ".jar");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setJarByClass(MergeTimeSeriesOf200EntityResult.class);
		
		if (phase == 0) {
			job.setMapperClass(DistinctMap.class);
			job.setNumReduceTasks(0);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(temp));
		} else {
			job.setMapperClass(MergeMap.class);
			job.setReducerClass(Reduce.class);
			FileInputFormat.addInputPath(job, new Path(temp));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
		}
		//job.setNumReduceTasks(1);

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job,
				org.apache.hadoop.io.compress.BZip2Codec.class);


		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static int phase = 0;
	public static String temp = "temp_folder";

	public static void main(String[] args) throws Exception {
		//phase = 0;
		//int res = ToolRunner.run(new Configuration(),
		//		new MergeTimeSeriesOf200EntityResult(), args);
		phase = 1;
		int res = ToolRunner.run(new Configuration(),
				new MergeTimeSeriesOf200EntityResult(), args);
		
		
		System.exit(res);
	}
}