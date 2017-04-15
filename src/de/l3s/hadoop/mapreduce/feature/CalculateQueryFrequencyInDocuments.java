package de.l3s.hadoop.mapreduce.feature;





import java.io.*;
import java.util.HashSet;
import java.util.Iterator;

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

public class CalculateQueryFrequencyInDocuments extends Configured implements Tool {
	
	private static String query = "";
	
	private static HashSet<String> entity_url = new HashSet<String>();

	private static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text textKey = null;

		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value != null) {
					String[] line = value.toString().split("\t");
					int count = 0;
					int current_pos = -1;
					if (line.length == 4 && line[2].length() > 0 && line[3].length() > 0) {
						line[2] = line[2].trim();
						if (!(entity_url.contains(line[2]) || 
								entity_url.contains(line[2] + "/") ||
								(entity_url + "/").contains(line[2])) )
							return;
						textKey = new Text(line[2]);
						while (line[3].length() >= query.length()) {
							current_pos = line[3].toLowerCase().indexOf(query);
							if (current_pos >= 0){
								count ++;
								line[3] = line[3].substring(current_pos + query.length(), line[3].length());
							}
							else
								break;
						}
						output.write(textKey, new IntWritable(count));
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

	private static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context output)
				throws IOException, InterruptedException {
			try {
				int inlink = 0;
				Iterator<IntWritable> iter = values.iterator();
				while (iter.hasNext()) {
					inlink  += iter.next().get();
				}
				output.write(key, new IntWritable(inlink));
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

		Job job = Job.getInstance(conf, "CalculateInlinkIndexDocuments");

		job.setJobName(this.getClass().getName() + ".jar");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setJarByClass(CalculateQueryFrequencyInDocuments.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job,
				org.apache.hadoop.io.compress.BZip2Codec.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		File f = new File(args[1]);
		BufferedReader br = new BufferedReader(new FileReader(f));
		query = f.getName().toLowerCase().replace('_', ' ');
		String rl = "";
		while ((rl = br.readLine()) != null) {
			entity_url.add(rl.trim());
		}
		System.out.println("Process: " + query + " with " + entity_url.size() + " url");
		int res = ToolRunner.run(new Configuration(),
				new CalculateQueryFrequencyInDocuments(), args);
		br.close();
		System.exit(res);
	}
}