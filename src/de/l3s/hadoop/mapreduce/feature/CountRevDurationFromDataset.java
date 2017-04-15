package de.l3s.hadoop.mapreduce.feature;


import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.PriorityQueue;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.DateTime;
import org.joda.time.Days;


public class CountRevDurationFromDataset extends Configured implements Tool {

	private static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text textKey = null;
		private DateTime BEGIN_DATE = DateTime.parse("1990-01-01"); 
		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value != null) {
					String[] line = value.toString().split("\t");
					if (line.length == 3 && line[1].length() > 0) {
						textKey = new Text(line[0].trim());
						String[] rev = line[1].split(" ");
						int duration = 0;
						DateTime d = null;
						PriorityQueue<Integer> spanSeries = new PriorityQueue<Integer>();
						for (int i = 0; i < rev.length; i ++) {
							d = DateTime.parse(rev[i].trim());
							duration = Days.daysBetween(BEGIN_DATE, d).getDays();
							spanSeries.add(duration);
						}
						
						int timeSpanCount = 0;
						duration = spanSeries.peek();
						int span = 0;
						
						while (!spanSeries.isEmpty()) {
							span = spanSeries.poll();
							if (Math.abs(span - duration) > 7) {
								timeSpanCount ++;
							}
							duration = span;
						}
						
						output.write(textKey, new Text("" + timeSpanCount + "\t" + line[2]));
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

		Job job = Job.getInstance(conf, "CountRevDurationFromDataset");

		job.setJobName(this.getClass().getName() + ".jar");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setJarByClass(CountRevDurationFromDataset.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job,
				org.apache.hadoop.io.compress.BZip2Codec.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new CountRevDurationFromDataset(), args);
		System.exit(res);
	}
}