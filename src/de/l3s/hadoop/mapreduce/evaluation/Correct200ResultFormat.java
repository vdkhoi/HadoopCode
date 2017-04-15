package de.l3s.hadoop.mapreduce.evaluation;

import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//	Format:
//	url TAB anchors TAB inlink

public class Correct200ResultFormat extends Configured implements Tool {

	

	private static class Map extends
			Mapper<LongWritable, Text, Text, NullWritable> {

		public static NullWritable NULL_VALUE = NullWritable.get();
		
		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value == null)
					return;
				String[] line = value.toString().split("\t");
				if (line.length == 23) {
					output.write(new Text(
								line[0]  + "\t" + line[1]  + "\t" + line[4]  + "\t" + line[17] + "\t" + 
								line[20] + "\t" + line[6]  + "\t" + line[13] + "\t" + line[18] + "\t" + 
								line[16] + "\t" + line[2]  + "\t" + line[3]  + "\t" + line[7]  + "\t" +
								line[22] + "\t" + line[9]  + "\t" + line[11] + "\t" + line[12] + "\t" +
								line[10] + "\t" + line[21] + "\t" + line[14] + "\t" + line[19] + "\t" +
								line[8]), NULL_VALUE);
				
				}
				else if (line.length == 21) {
					output.write(new Text(value.toString()), NULL_VALUE );					
				}
			} catch (Exception e2) {
				throw new IOException(value.toString(), e2);
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

		Job job = Job.getInstance(conf, "Correct200ResultFormat");

		job.setJobName(this.getClass().getName() + ".jar");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setJarByClass(Correct200ResultFormat.class);
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
		int res = ToolRunner.run(new Configuration(), new Correct200ResultFormat(),
				args);
		System.exit(res);
	}
}