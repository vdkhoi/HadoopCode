package de.l3s.hadoop.mapreduce.index.archive;

import java.io.IOException;
import java.util.regex.Pattern;
//import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.io.compress.bzip2.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ProcessAnchors extends Configured implements Tool{

	private static final int CONTROL_LIMIT = 50;
	
	public static class AnchorMapper
	extends Mapper<Object, Text, Text, IntWritable>{

		private Text word = new Text();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			String[] fields = null;
			try {
				fields = value.toString().split("[\t ]");
				String anchors = "";
				int len = fields.length;
				if (len > 1000) len = 1000;
				for (int i = 1; i < len; i++) {
					anchors += ( fields[i] + (i == len? "" : " ") );
				}
				word.set(fields[0] + "\t" + anchors);
				fields = null;
				context.write(word, null);
			} catch (Exception ex) {
				if (ex instanceof IOException)
					throw new IOException("Error: ", ex);
				else if (ex instanceof InterruptedException)
					throw new InterruptedException("Error: " + ex.toString());
				else if (ex instanceof Exception)
					throw new IOException("Error: " + fields[0] + "\r\n" + fields[1], ex);
			}
		}
		
		private String normalizeURL (String input) {
			String str = input;
			int i = str.indexOf(")");
			int j = str.indexOf(",");
			if ((j > i - 1) || (j < 0)) return null; 
			String rest = str.substring(i + 1, str.length());
			str = str.replace(")", "");
			str = str.substring(0, i);
			String[] values = str.split(",");
			str = "";
			for (i = values.length - 1; i > 0; i--)
				str = str + values[i] + ".";
			str = str + values[0] + rest;
			values = null;
			rest = null;
			return str;
		}
		
		private String processAnchorsTuple(String str, String addr){
			int len;
			if ((len = str.length()) > 4)
				str = str.substring(1, len - 1);
			else
				return null;
			
			String pattern = Pattern.quote("(" + addr + ",");
			String[] values = str.split(pattern, CONTROL_LIMIT);
			if (values.length == CONTROL_LIMIT) {
				values[CONTROL_LIMIT - 1] = null;
				len = CONTROL_LIMIT - 1;
			}
			else {
				len = values.length;
			}
			str = "";
			int i = 0, k = 0;		
			for (i = 0; i < len; i++) {
				if (values[i].length() == 0) {
					k ++;
					continue; 
				}
				
				if (i < len - 1)
					values[i] = values[i].substring(0, values[i].length() - 2);
				else
					values[i] = values[i].substring(0, values[i].length() - 1);
				
				str += (values[i] + ((i == values.length - 1)? "" : " | "));
				
//				if (i == CONTROL_LIMIT) {
//					str += values[i];
//					break;
//				}
			}
			values = null;
			return str + "\t" + (i - k) + "\t" + len;
		}
	}
	
	public static class IntSumReducer
	extends Reducer<Text,IntWritable,Text,IntWritable> {
		//private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
		}
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ProcessAnchors(), args);
        System.exit(res);
	}

	public int run(String[] args) throws Exception {
		//Configuration conf = new Configuration();
		Configuration conf = this.getConf();
		conf.setBoolean("map.output.compress", true);
		conf.set("mapreduce.output.compression.type", "BLOCK");
		//conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
		conf.setClass("mapreduce.output.fileoutputformat.compress.codec", org.apache.hadoop.io.compress.BZip2Codec.class, org.apache.hadoop.io.compress.CompressionCodec.class);
		conf.set("mapreduce.task.timeout", "1800000");
		conf.set("mapreduce.map.java.opts", "-Xmx3072m");
		conf.set("mapreduce.child.java.opts", "-Xmx3072m");

		//SET mapred.output.compression.codec org.apache.hadoop.io.compress.BZip2Codec';
		Job job = Job.getInstance(conf, "Process Anchors");
		job.setJarByClass(ProcessAnchors.class);
		job.setMapperClass(AnchorMapper.class);
		job.setNumReduceTasks(0);
		//job.setCombinerClass(IntSumReducer.class);
		//job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.BZip2Codec.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}