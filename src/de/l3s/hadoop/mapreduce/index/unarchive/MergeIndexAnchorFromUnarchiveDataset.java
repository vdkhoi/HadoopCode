package de.l3s.hadoop.mapreduce.index.unarchive;


import gnu.trove.map.hash.TObjectIntHashMap;


import java.io.*;
import java.util.Iterator;

import org.apache.commons.lang.StringEscapeUtils;
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

// Format: core_url_id TAB dest_url TAB merged_anchor TAB inlink

public class MergeIndexAnchorFromUnarchiveDataset extends Configured implements Tool {
	public static final int MAX_LENGTH = 100000;

	private static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text textKey = null;
		private Text textValue = null;

		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value != null) {
					String[] line = value.toString().split("\t");
					if (line.length == 5 && line[4].length() > 0) {  // Not count blank space
						textKey = new Text(line[0].trim() + "\t" + line[3].trim());
						textValue = new Text(
								StringEscapeUtils.unescapeHtml(line[4].trim()));
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
				String anchors = "", currentAnchor = "";
				TObjectIntHashMap<String> anchor_list = new TObjectIntHashMap<String>();
				int len = 0;
				int inlink = 0;
				Iterator<Text> iter = values.iterator();
				while (iter.hasNext()) {
					currentAnchor = iter.next().toString();
					inlink ++;
					if ((len = anchor_list.get(currentAnchor)) < MAX_LENGTH) {
						anchor_list.adjustOrPutValue(currentAnchor, len + 1, 1);
						if (anchors.length() + currentAnchor.length() < 1000000)
							anchors += (currentAnchor + " ");
					}
				}
				output.write(key, new Text(anchors + "\t" + inlink));
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

		Job job = Job.getInstance(conf, "MergeIndexAnchorFromUnarchiveDataset");

		job.setJobName(this.getClass().getName() + ".jar");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setJarByClass(MergeIndexAnchorFromUnarchiveDataset.class);
		job.setMapperClass(Map.class);
		//job.setCombinerClass(Reduce.class);  Error here
		job.setReducerClass(Reduce.class);

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job,
				org.apache.hadoop.io.compress.BZip2Codec.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new MergeIndexAnchorFromUnarchiveDataset(), args);
		System.exit(res);
	}
}