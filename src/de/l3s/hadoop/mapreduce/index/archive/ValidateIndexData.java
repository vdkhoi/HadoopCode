package de.l3s.hadoop.mapreduce.index.archive;

import java.io.*;
import java.util.Iterator;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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


public class ValidateIndexData extends Configured implements Tool {
	private static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
				
		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			String row = StringEscapeUtils.unescapeHtml(value.toString().toLowerCase());
			String[] line = row.split("\t", 4);
			
			if 	(line != null) {
				if (line.length == 4)
					output.write(new Text(line[1] + "\t" + line[2]),  new IntWritable(1));
			}
			
		}
	}

	private static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,	Context output)
				throws IOException, InterruptedException {
			
			Iterator<IntWritable> iter = values.iterator();
			int count = 0;
			while (iter.hasNext()) {
				count += iter.next().get();
			}
			if (count > 1)
				output.write(key, new IntWritable(count));
		}
	}

	
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		

		conf.setBoolean("map.output.compress", true);
		conf.set("mapreduce.output.compression.type", "BLOCK");
		conf.setClass("mapreduce.output.fileoutputformat.compress.codec", org.apache.hadoop.io.compress.BZip2Codec.class, org.apache.hadoop.io.compress.CompressionCodec.class);
		conf.set("mapreduce.task.timeout", "1800000");
		conf.set("mapreduce.map.java.opts", "-Xmx3072m");
		conf.set("mapreduce.child.java.opts", "-Xmx3072m");
        //conf.setInputFormat(TextInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);
		
		Job job = Job.getInstance(conf, "BuildIndexData");
		
		job.setJobName(this.getClass().getName() + ".jar");
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		job.setJarByClass(ValidateIndexData.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.BZip2Codec.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ValidateIndexData(), args);
		System.exit(res);
	}
}