package de.l3s.hadoop.mapreduce.graph;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.io.*;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//Prepare datasets:
//step1: Fetch root set from Bing
//step2: Convert Bing result to SURT form
//step3: Build base set from map/reduce   (this program)

public class GenerateDenseIDForArchiveURL extends Configured implements Tool {

	private static class Map extends
			Mapper<LongWritable, Text, Text, NullWritable> {

		static int file_number, start_id;

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			String line[] = conf.get("line_count_of_file").split("\t");
			TIntArrayList line_count_of_file;
			line_count_of_file = new TIntArrayList();
			for (int i = 0; i < line.length / 2; i++) {
				line_count_of_file.add(Integer.parseInt(line[2 * i + 1]));
			}

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			filename = filename.replace("part-r-", "").replace(".bz2", "");
			file_number = Integer.parseInt(filename);
			if (file_number > 0) {
				int last_count = 0;
				for (int i = 0; i < file_number; i++) {
					last_count += line_count_of_file.get(i);
				}
				start_id = last_count;

			} else
				start_id = 0;
		}

		private static NullWritable NULL = NullWritable.get();

		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value == null)
					return;
				String row = value.toString();
				if (row == null)
					return;

				output.write(new Text("" + start_id + "\t" + row), NULL);

				start_id++;

			} catch (Exception e) {
				throw new IOException(value.toString(), e);
			}
		}
	}

//	private static class Reduce extends
//			Reducer<Text, NullWritable, Text, LongWritable> {
//		@Override
//		protected void reduce(Text key, Iterable<LongWritable> values,
//				Context output) throws IOException, InterruptedException {
//			Iterator<LongWritable> iter = values.iterator();
//			long sum = 0;
//			while (iter.hasNext()) {
//				sum += iter.next().get();
//			}
//			output.write(key, new LongWritable(sum));
//		}
//	}

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

		File r = new File(args[2]);
		BufferedReader br = new BufferedReader(new FileReader(r));
		String rl = "", line_count_of_file = "";
		while ((rl = br.readLine()) != null) {
			line_count_of_file += (rl + "\t");
		}
		br.close();

		conf.set("line_count_of_file", line_count_of_file);

		Job job = Job.getInstance(conf, this.getClass().getName());

		job.setJobName(this.getClass().getName() + ".jar");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setJarByClass(GenerateDenseIDForArchiveURL.class);
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

		int res = ToolRunner.run(new Configuration(),
				new GenerateDenseIDForArchiveURL(), args);
		System.exit(res);

	}
}