package de.l3s.hadoop.mapreduce.index.archive.recover;

// Feature 3, 5, 15



import java.io.*;
import java.util.HashSet;
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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ComputePageRankFeaturesStep2 extends Configured implements Tool {
	public static final int DATA_MAP_1 = 1;
	public static final int DATA_MAP_2 = 2;

	private static class LoadDataMap extends Mapper<LongWritable, Text, Text, Text> {
		private Text textKey = null;
		private Text textValue = null;

		// IN: domain_ID TAB Core_URL_ID TAB URL || TAB URL
		// OUT: <domain_ID , Core_URL_ID TAB URL>
		
		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value != null) {
					String row = value.toString();
					String[] line = value.toString().split("\t", 2);

					if (line.length > 1 && line[0].length() > 0) {
						textKey = new Text(line[0]);
						textValue = new Text(DATA_MAP_1 + ":" + line[1]);
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
	
	
	private static class LoadIndexListMap extends Mapper<LongWritable, Text, Text, Text> {
		private Text textKey = null;
		private Text textValue = null;

		// IN: Domain_ID TAB PR_Score
		
		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value != null) {
					String[] line = value.toString().split("\t", 2);

					if (line.length > 1) {
						textKey = new Text(line[0]);
						textValue = new Text(DATA_MAP_2 + ":" + line[1]);
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
	

	private static class Reduce extends Reducer<Text, Text, Text, NullWritable> {

		public static final NullWritable NULL = NullWritable.get();
		
		public static int FIELD_TO_APPEND = 0;
		
		@Override
		protected void setup(Context context) throws IOException ,InterruptedException {
			Configuration conf = context.getConfiguration();
			FIELD_TO_APPEND = conf.getInt("FIELD_TO_APPEND", 1);
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context output)
				throws IOException, InterruptedException {
			try {
				Iterator<Text> iter = values.iterator();
				String current, score_pagerank = "";
				
				HashSet<String> data_rows = new HashSet<String>();
				
				boolean hasScore = false;
				
				while (iter.hasNext()) {
					current = iter.next().toString();
					int id = Integer.parseInt(current.substring(0, 1));
					current = current.substring(2, current.length());
					if (id == DATA_MAP_1) {
						if (!hasScore)
							data_rows.add(current);
						else {
							output.write(new Text(current + "\t" + score_pagerank), NULL);
						}
					}
					else {
						score_pagerank = current;
						hasScore = true;
					}
				}
				
				if (!hasScore) {
					if (FIELD_TO_APPEND > 0) {
						for(int i = 0; i < FIELD_TO_APPEND - 1; i ++) {
							score_pagerank += "n/a\t";
						}
						score_pagerank += "n/a";
						for (String dat : data_rows) {
							output.write(new Text(dat + "\t" + score_pagerank), NULL);
						}
					}
					//discarded else
				}
				else {
					for (String dat : data_rows) {
						output.write(new Text(dat + "\t" + score_pagerank), NULL);
					}
				}
				
			
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
		conf.setInt("FIELD_TO_APPEND", Integer.parseInt(args[3]));

		Job job = Job.getInstance(conf, this.getClass().getName());

		job.setJobName(this.getClass().getName() + ".jar");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		

		job.setJarByClass(ComputePageRankFeaturesStep2.class);
		job.setMapperClass(LoadDataMap.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job,
				org.apache.hadoop.io.compress.BZip2Codec.class);

		
		
		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, LoadDataMap.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, LoadIndexListMap.class);
		
		
//		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new ComputePageRankFeaturesStep2(), args);
		System.exit(res);
	}
}