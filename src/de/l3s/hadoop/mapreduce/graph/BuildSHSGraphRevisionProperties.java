package de.l3s.hadoop.mapreduce.graph;

// Build adjacency list of time revision for German archive year 2013 

import java.io.*;
import java.util.PriorityQueue;
import java.util.regex.Pattern;

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
import org.joda.time.*;

//hadoop jar /home/khoi/jars/HadoopCode-0.0.1-SNAPSHOT-jar-with-dependencies.jar de.l3s.hadoop.mapreduce.graph.BuildSHSGraphRevisionProperties SHS_graph/graph_year_2013 SHS_graph/graph_year_2013_by_difference

public class BuildSHSGraphRevisionProperties extends Configured implements Tool {
	
	private static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {

		
		
		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value != null) {
					// need to sort timestamp
					
					
					output.write(key, value);
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

	private static class Reduce extends Reducer<LongWritable, Text, Text, NullWritable> {

		public String getDest_url(String decodeURL) {
	    	
	    	String first = "", later = "", revfirst = "";
	    	int split_pos = decodeURL.indexOf(')');
	    	if ( split_pos >= 4){
	    		first = decodeURL.substring(0, split_pos);
	    		String[] dn = first.split(",");
	    		int len = dn.length;
	    		revfirst = dn[0];
	    		for (int i = 1; i < len; i++) {
	    			revfirst = dn[i] + "." + revfirst;
	    		}
	    		later = decodeURL.substring(split_pos + 1);
	    	}
	        return ("http://" + revfirst + later);
	    }
		
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values, Context output)
				throws IOException, InterruptedException {
			try {
				String row = values.iterator().next().toString();
				String[] cols = row.split("\t", 3);
				String[] time_series = cols[1].split(" ");
				
				PriorityQueue<String> time_series_sort = new PriorityQueue<String>();
				for (int i = 0; i < time_series.length; i++) {
					time_series_sort.add(time_series[i]);
				}
				String time_diff = time_series[0]; 
				
				DateTime first_rev = new DateTime(time_series_sort.poll());
				DateTime curr = null, prev = first_rev;
				while(time_series_sort.size() > 0) {
					curr = new DateTime(time_series_sort.poll());
					time_diff += (" " + Seconds.secondsBetween(prev, curr).getSeconds());
					prev = curr;
				}
				
				output.write(new Text(getDest_url(cols[0]) + "\t" + time_diff + "\t" + cols[2]), NullWritable.get());
				
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
				org.apache.hadoop.io.compress.GzipCodec.class,
				org.apache.hadoop.io.compress.CompressionCodec.class);
		conf.set("mapreduce.task.timeout", "1800000");
		conf.set("mapreduce.map.java.opts", "-Xmx3072m");
		conf.set("mapreduce.child.java.opts", "-Xmx3072m");

		Job job = Job.getInstance(conf, this.getClass().getName());

		job.setJobName(this.getClass().getName() + ".jar");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setJarByClass(this.getClass());
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
				new BuildSHSGraphRevisionProperties(), args);
		System.exit(res);
	}
}