package de.l3s.hadoop.mapreduce.feature.word2vec.apply;

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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/* Input

Repeat many times with a pair <doc_id, query_entity>
#1: doc_id
#2: query_entity
#3: WordVector of a term
...
#102: WWordVectorValue of a terms
End repeat

 */

/* Output

#1: doc_id
#2: query_entity
#3: Sum WordVector value of all terms in anchors
...
#102: Sum WordVector value of all terms in anchors


WordVectorValues

*/

/*
 * Syntax
 * hadoop jar /home/khoi/jars/HadoopCode-0.0.1-SNAPSHOT-jar-with-dependencies.jar de.l3s.hadoop.mapred.feature.word2vec.apply.BuildWord2VecFeatureFromAnchors evaluation/200_result_es_enhanced_features_add_two_features/Bibra evaluation/word2vec_vector temp_debug_wordvector 
 */

public class MergeWordVector2Result extends Configured implements Tool {
	
	public static int WORD_VECTOR_LENGTH = 100;

	private static class LoadDocumentMapper extends
			Mapper<LongWritable, Text, LongWritable, Text> {
		
		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value == null)
					return;
				String row = value.toString();
				if (row == null)
					return;
				String[] line = row.split("[\t ]", 2);
				output.write(new LongWritable(Long.parseLong(line[0])), new Text("doc\t" + row));
			} catch (Exception e) {
				throw new IOException(value.toString(), e);
			}
		}
	}

	private static class LoadVectorMapper extends
			Mapper<LongWritable, Text, LongWritable, Text> {

		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value == null) 
					return;
				String row = value.toString();
				if (row == null)
					return;
				String[] line = row.split("\t", 2);
				
				output.write(new LongWritable(Long.parseLong(line[0])), new Text("vec\t" + line[1]));
			} catch (Exception e) {
				throw new IOException(value.toString(), e);
			}
		}
	}
	
	public static class JoinReducer extends Reducer<LongWritable, Text, Text, Text> {
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,
				Context output) throws IOException, InterruptedException {
			
			Iterator<Text> iter = values.iterator();
			String curr = "", row = "";
			String vector_string = "";
			String[] line = null;
			
			while (iter.hasNext()) {
				curr = iter.next().toString();
				
				line = curr.split("[\t ]", 2);
				if (line[0].equals("doc")) {
					row = line[1];
				}
				
				if (line[0].equals("vec")) {
					vector_string = line[1];
				}

				
			}
			
			if (vector_string.length() == 0 && row.length() > 0){
				for (int i = 0; i < WORD_VECTOR_LENGTH - 1; i++) {
					vector_string += ("0.0\t");
				}
				vector_string += ("0.0");
			}
				
			if (row.length() > 0) {
				output.write(new Text(row), new Text(vector_string));
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
		Job job = Job.getInstance(conf, this.getClass().getName());
		job.setJobName(this.getClass().getName() + ".jar");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setJarByClass(MergeWordVector2Result.class);
		job.setReducerClass(JoinReducer.class);
		//job.setMapperClass(VectorMapper.class);
		//jobsetNumReduceTasks(0);
		

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job,
				org.apache.hadoop.io.compress.BZip2Codec.class);

		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, LoadDocumentMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, LoadVectorMapper.class);
		
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(),
				new MergeWordVector2Result(), args);
		System.exit(res);
	}
}