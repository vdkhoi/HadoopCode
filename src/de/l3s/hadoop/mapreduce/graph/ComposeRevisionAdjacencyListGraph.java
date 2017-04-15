package de.l3s.hadoop.mapreduce.graph;

import gnu.trove.list.array.TByteArrayList;
import gnu.trove.list.array.TIntArrayList;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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

//hadoop jar /home/khoi/jars/HadoopCode-0.0.1-SNAPSHOT-jar-with-dependencies.jar de.l3s.hadoop.mapreduce.graph.ComposeRevisionAdjacencyListGraph SHS_graph/graph_adjcency_2013_Jan_new/part-r-00133.bz2 SHS_graph/time_matrix

public class ComposeRevisionAdjacencyListGraph extends Configured implements Tool {
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
					String[] line = value.toString().split("\t", 2);
					textKey = new Text(line[0]);
					textValue = new Text(line[1]);
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
		
		private class TimeStampAdjList implements Comparable<TimeStampAdjList> {
		    private HashSet<String> value = null;
		    private String key;

		    public TimeStampAdjList(String key, HashSet<String> value) {
		        this.setKey(key);
		        this.setValue(value);
		    }

		    @Override
		    public int compareTo(TimeStampAdjList o) {
		    	return this.getKey().compareTo(o.getKey());
		    }

			public HashSet<String> getValue() {
				return value;
			}

			public void setValue(HashSet<String> _value) {
				this.value = _value;
			}

			private String getKey() {
				return key;
			}

			private void setKey(String key) {
				this.key = key;
			}
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context output)
				throws IOException, InterruptedException {
			try {
				PriorityQueue<String> outlinkQueue = new PriorityQueue<String>();
				Iterator<Text> iter = values.iterator();
				String currentOutlink = null;
				String[] fields = null;
				while (iter.hasNext()) {
					currentOutlink = iter.next().toString();
					outlinkQueue.add(currentOutlink);
				}
				int i = 0;
				PriorityQueue<TimeStampAdjList> adjList = new PriorityQueue<TimeStampAdjList>();
				PriorityQueue<String> allOutlinks = new PriorityQueue<String>();
				while (outlinkQueue.size() > 0)
				{
					currentOutlink = outlinkQueue.poll();
					fields = currentOutlink.split("[\t ]");
					HashSet<String> vertices = new HashSet<String>();
					for (i = 0; i < fields.length - 2; i ++){
						vertices.add(fields[i + 2]);
						allOutlinks.add(fields[i + 2]);
					}
					adjList.add(new TimeStampAdjList(fields[0], vertices));  // timestamp, vertices
				}
				
				if (allOutlinks.size() == 0) return;
				
				String[] allOutlinkArray = new String[allOutlinks.size()];
				allOutlinks.toArray(allOutlinkArray);
				String allOutlinkString = allOutlinkArray[0];
				for (i = 1; i < allOutlinkArray.length; i++) {
					allOutlinkString += (" " + allOutlinkArray[i]);
				}
				
				int[] adjcencyBits = new int[(allOutlinkArray.length / 32) + (allOutlinkArray.length % 32 > 0? 1 : 0) ];
				String existLink = new String();
				TimeStampAdjList currentAdjList = null;
				HashSet<String> currentAdjVertexList = null;
				
				String matrix_encoding = "" + adjcencyBits + " ";
				
				int curr_nybble = 0;
				
				while (adjList.size() > 0) {
					existLink = "";
					curr_nybble = 0;
					currentAdjList = adjList.poll();    // timestamp, vertices
					currentAdjVertexList = currentAdjList.getValue();
					for (i = 0; i < allOutlinkArray.length; i++) {
						
						if (currentAdjVertexList.contains(allOutlinkArray[i])){
							existLink = "1" + existLink;
						}
						else{
							existLink += "0" + existLink;
						}
						
						// compress into integer value
						if (existLink.length() == 32 && curr_nybble < adjcencyBits.length){
							adjcencyBits[curr_nybble ++] = Integer.parseUnsignedInt(existLink, 2); 
							existLink = "";
						}
						
					}
					if (existLink.length() > 0 && i % 32 > 0 && curr_nybble < adjcencyBits.length)
						adjcencyBits[curr_nybble] = Integer.parseUnsignedInt(existLink, 2); 
					matrix_encoding += currentAdjList.getKey() + " ";
					for (i = 0; i < adjcencyBits.length; i++) {
						matrix_encoding += (adjcencyBits[i] + " ");
					}
				}
				
				output.write(key, new Text(matrix_encoding + "\t" + allOutlinkString));
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

		job.setJarByClass(ComposeRevisionAdjacencyListGraph.class);
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
				new ComposeRevisionAdjacencyListGraph(), args);
		System.exit(res);
	}
}