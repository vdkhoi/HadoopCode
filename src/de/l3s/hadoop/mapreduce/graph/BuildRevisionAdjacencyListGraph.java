package de.l3s.hadoop.mapreduce.graph;

import java.io.*;
import java.util.Iterator;
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

//hadoop jar /home/khoi/jars/HadoopCode-0.0.1-SNAPSHOT-jar-with-dependencies.jar de.l3s.hadoop.mapreduce.graph.BuildRevisionAdjacencyListGraph SHS_graph/graph_links_2013_Jan SHS_graph/graph_adjcency_2013_Jan

public class BuildRevisionAdjacencyListGraph extends Configured implements Tool {
	public static final int MAX_LENGTH = 10000;
	
	public static String getDest_url(String decodeURL) {
    	
    	String first = "", later = "", revfirst = "";
    	int split_pos = decodeURL.indexOf(')');
    	if ( split_pos >= 4){
    		try 
    		{
	    		first = decodeURL.substring(0, split_pos);
	    		String[] dn = first.split(",");
	    		int len = dn.length;
	    		revfirst = dn[0];
	    		int i = 1;
	    		for (i = 1; i < len && dn[i].length() > 0; i++) {
	    			revfirst = dn[i] + "." + revfirst;
	    		}
	    		later = decodeURL.substring(split_pos + 1);
	    		if (i == len)
	    			return ("http://" + revfirst + later);
    		}
    		catch (Exception ex) 
    		{
    			return null;
    		}
    	}
        return null;
    }


	private static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text textKey = null;
		private Text textValue = null;
		
		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value != null) {
					String[] line = value.toString().split("\t", 4);
					String src = getDest_url(line[0].trim());
					String dst = getDest_url(line[2].trim());
					if (src != null && dst != null) {
						textKey = new Text(src + "\t" + line[1].trim());
						textValue = new Text(dst);
						output.write(textKey, textValue);
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

	private static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context output)
				throws IOException, InterruptedException {
			try {
				String outlinkList = "", currentOutlink = "";
				
				PriorityQueue<String> outlinkQueue = new PriorityQueue<String>();
				Iterator<Text> iter = values.iterator();
				int inlink = 0;
				while (iter.hasNext() && inlink++ < 10000) {
					currentOutlink = iter.next().toString();
					outlinkQueue.add(currentOutlink);
				}
				inlink = outlinkQueue.size();
				while (outlinkQueue.size() > 0)
				{
					currentOutlink = outlinkQueue.poll();
					outlinkList += (currentOutlink + " ");
				}
				output.write(key, new Text(inlink + "\t" + outlinkList.trim()));
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

		Job job = Job.getInstance(conf, "MergeTimeStampFromDataset");

		job.setJobName(this.getClass().getName() + ".jar");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setJarByClass(BuildRevisionAdjacencyListGraph.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		//job.setNumReduceTasks(1);

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job,
				org.apache.hadoop.io.compress.BZip2Codec.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new BuildRevisionAdjacencyListGraph(), args);
		System.exit(res);
	}
}