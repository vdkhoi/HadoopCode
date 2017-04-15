package de.l3s.hadoop.mapreduce.graph.hits.prepare;

import gnu.trove.map.hash.TIntObjectHashMap;

import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

//Prepare datasets:
//step1: Fetch root set from Bing
//step2: Convert Bing result to SURT form
//step3: Build base set from map/reduce   (this program)

public class BuildGraphSubsetFromSeeds extends Configured implements Tool {

	private static class Map extends
			Mapper<LongWritable, Text, Text, NullWritable> {

		private TIntObjectHashMap<String> seeds;

		int hash_base = 3;

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			String line[] = conf.get("seeds").split("\t");
			seeds = new TIntObjectHashMap<String>();
			boolean collision = true;
			int shingling_value;
			while (collision) {
				collision = false;
				for (int i = 0; i < line.length / hash_base; i++) {
					shingling_value = getShinglingValue(line[i], hash_base);
					if (seeds.contains(shingling_value)) {
						collision = true;
						hash_base++;
						break;
					}
					seeds.put(shingling_value, line[i]);
					if (seeds.contains(shingling_value + '/')) {
						collision = true;
						hash_base++;
						break;
					}
					seeds.put(line[i].hashCode(), line[i]);
				}
			}
		}

		private static NullWritable NULL = NullWritable.get();

		public int getShinglingValue(String url, int hash) {
			int sum = 0;
			for (int i = 0; i < url.length() / hash; i++) {
				sum += url.charAt(i * hash);
			}
			return sum;
		}

		private boolean matchInLinkOrOutLink(String link) {
			String[] line = link.split("\t");

			// Match outlink
			int hashCode = getShinglingValue(line[0], hash_base);
			if (seeds.contains(hashCode)) {
				String bing_url = seeds.get(hashCode);
				if (bing_url.equals(line[0]))
					return true;
			}
			
			if (seeds.contains(hashCode + '/')) {
				String bing_url = seeds.get(hashCode + '/');
				if (bing_url.equals(line[0]))
					return true;
			}

			// Match inlink
			hashCode = getShinglingValue(line[2], hash_base);
			if (seeds.contains(hashCode)) {
				String bing_url = seeds.get(hashCode);
				if (bing_url.equals(line[2]))
					return true;
			}
			
			if (seeds.contains(hashCode + '/')) {
				String bing_url = seeds.get(hashCode + '/');
				if (bing_url.equals(line[2]))
					return true;
			}

			return false;
		}

		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value == null)
					return;
				String row = value.toString();
				if (row == null)
					return;

				if (matchInLinkOrOutLink(row)) {
					output.write(new Text(row), NULL);
				}

			} catch (Exception e) {
				throw new IOException(value.toString(), e);
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

		File r = new File(args[2]);
		BufferedReader br = new BufferedReader(new FileReader(r));
		String rl = "", seeds = "";
		while ((rl = br.readLine()) != null) {
			seeds += (rl + "\t");
		}
		br.close();

		conf.set("seeds", seeds);

		Job job = Job.getInstance(conf, this.getClass().getName());

		job.setJobName(this.getClass().getName() + ".jar");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setJarByClass(BuildGraphSubsetFromSeeds.class);
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
				new BuildGraphSubsetFromSeeds(), args);
		System.exit(res);

	}
}