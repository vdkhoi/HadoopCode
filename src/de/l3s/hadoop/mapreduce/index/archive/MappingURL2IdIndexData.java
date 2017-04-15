package de.l3s.hadoop.mapreduce.index.archive;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MappingURL2IdIndexData extends Configured implements Tool {
	
	
	
	private static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
		
		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				output.write(key, value);
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
		
		Job job = Job.getInstance(conf, "MappingURL2IdIndexData");
		
		job.setJobName(this.getClass().getName() + ".jar");
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		
		job.setJarByClass(MappingURL2IdIndexData.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		//job.setCombinerClass(Reduce.class);
		//job.setReducerClass(Reduce.class);
		
		FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.BZip2Codec.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new MappingURL2IdIndexData(), args);
		System.exit(res);

	}
}