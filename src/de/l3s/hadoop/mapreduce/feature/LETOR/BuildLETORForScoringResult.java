package de.l3s.hadoop.mapreduce.feature.LETOR;

import gnu.trove.map.hash.TObjectIntHashMap;

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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class BuildLETORForScoringResult extends Configured implements Tool {
	public static NullWritable NULL = NullWritable.get(); 
	
	private static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		
		TObjectIntHashMap<String> query_id_map = new TObjectIntHashMap<String>();
		
		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			String[] pair_string = conf.get("query_id_map").split("\t");
			for (int i = 0; i < pair_string.length / 2; i++) {
				query_id_map.put(pair_string[2 * i + 1],
						Integer.parseInt(pair_string[2 * i]));
			}
		}

		// Fields to remove
		public String buildLETOR(String data) {
			String[] line = data.split("\t");
			String out_letor = "";
			int feature_num = 1, i = 1;
			for (i = 1; i < line.length; i ++) {
				if (i == 17 || i == 18) continue;
				out_letor += (feature_num + ":" + line[i] + " ");
				feature_num ++;
			}
						
			// Add detail
			out_letor += ("#" + line[0]);
			out_letor = "0 qid:" + query_id_map.get(line[18]) + " " + out_letor;
			
			return out_letor;
		}
		
		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				String row = value.toString();
				if (row == null) return;
				output.write(new Text(buildLETOR(row)), NULL);
				
			} catch (Exception e) {
				throw new IOException(value.toString(), e);
			}
		}
	}
	
	private static class Reduce extends Reducer<Text, NullWritable, Text, NullWritable> {

		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context output)
				throws IOException, InterruptedException {
			try {

				output.write(key, NULL);
					
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
		conf.setClass("mapreduce.output.fileoutputformat.compress.codec", org.apache.hadoop.io.compress.BZip2Codec.class, org.apache.hadoop.io.compress.CompressionCodec.class);
		conf.set("mapreduce.task.timeout", "1800000");
		conf.set("mapreduce.map.java.opts", "-Xmx3072m");
		conf.set("mapreduce.child.java.opts", "-Xmx3072m");
		conf.set("query_id_map", loadFile(args[2]));
		
		Job job = Job.getInstance(conf, "MappingURL2IdUnarchiveIndexData");
		
		job.setJobName(this.getClass().getName() + ".jar");
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setJarByClass(BuildLETORForScoringResult.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);
		
		FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.BZip2Codec.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public String loadFile(String filePath) {
		File f = new File(filePath);
		String result = "", rl = "";
		try {
			BufferedReader br = new BufferedReader(new FileReader(f));
			while ((rl = br.readLine()) != null) {
				result += (rl + "\t");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}

	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new BuildLETORForScoringResult(), args);
		System.exit(res);

	}
}