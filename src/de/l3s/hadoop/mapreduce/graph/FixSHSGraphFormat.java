package de.l3s.hadoop.mapreduce.graph;

// Build outlink graph for whole German archive 

import java.io.*;
import java.util.PriorityQueue;
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

//hadoop jar /home/khoi/jars/HadoopCode-0.0.1-SNAPSHOT-jar-with-dependencies.jar de.l3s.hadoop.mapreduce.graph.FixSHSGraphFormat whole_de_graph/adjacent_graph_prefix_sort_of_all whole_de_graph/adjacent_graph_prefix_sort_of_all_fix

public class FixSHSGraphFormat extends Configured implements Tool {
	
	private static class Map extends Mapper<LongWritable, Text, Text, Text> {

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
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value != null) {
					String[] line = value.toString().split("[\t ]");
					PriorityQueue<String> data_rows = new PriorityQueue<String>();
					Text textKey = new Text(getDest_url(line[0]));
					
					for (int i = 0; i < line.length - 2; i ++ ) {
						data_rows.add(line[i + 2]);
					}
					
					String return_value = (line.length - 2) + " " + getDest_url(data_rows.poll());
					
					while (!data_rows.isEmpty()) {
						return_value += (" " + getDest_url(data_rows.poll()));
					}

					Text textValue = new Text (return_value);
					
					output.write(textKey, textValue);
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

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context output)
				throws IOException, InterruptedException {
			try {

				output.write(key, values.iterator().next());
					
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

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setJarByClass(FixSHSGraphFormat.class);
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
				new FixSHSGraphFormat(), args);
		System.exit(res);
	}
}