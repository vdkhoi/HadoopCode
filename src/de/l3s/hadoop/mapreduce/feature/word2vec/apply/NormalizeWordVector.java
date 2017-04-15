package de.l3s.hadoop.mapreduce.feature.word2vec.apply;

import java.io.*;
import java.math.BigDecimal;
import java.util.Iterator;

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

public class NormalizeWordVector extends Configured implements Tool {
	
	public static int WORD_VECTOR_LENGTH = 100;

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
				String[] line = row.split("[\t ]", 2);
				
				output.write(new LongWritable(Long.parseLong(line[0])), new Text(line[1]));
			} catch (Exception e) {
				throw new IOException(value.toString(), e);
			}
		}
	}
	
	public static BigDecimal[] extractVector(String strVec) {
		BigDecimal[] result = new BigDecimal[WORD_VECTOR_LENGTH];
		String[] line = strVec.split("[\t ]");
		for (int i = 0; i < WORD_VECTOR_LENGTH; i ++) {
			if (line[i].length() > 0)
				result[i] = new BigDecimal(line[i]);
			else
				result[i] = new BigDecimal(0.0f);
		}
		return result;
	}
	
	public static BigDecimal[] addTwoVector(BigDecimal[] v1, BigDecimal[] v2) {
		BigDecimal[] result = new BigDecimal[WORD_VECTOR_LENGTH];
		if ((v1.length != WORD_VECTOR_LENGTH) || (v2.length != WORD_VECTOR_LENGTH))
			return null;
		for (int i = 0; i < WORD_VECTOR_LENGTH; i ++) {
			result[i] = v1[i].add(v2[i]);
		}
		return result;
	}

	public static class AccumulateReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,
				Context output) throws IOException, InterruptedException {
			String curr = "";
			try {
				Iterator<Text> iter = values.iterator();
				BigDecimal[] vector_values = new BigDecimal[WORD_VECTOR_LENGTH];
				
				//Init values
				for (int i = 0; i < WORD_VECTOR_LENGTH; i++) {
					vector_values[i] = new BigDecimal(0.0f);
				}
				
				
				while (iter.hasNext()) {
					curr = iter.next().toString();
					vector_values = addTwoVector(vector_values, extractVector(curr));
				}
				
				String vector_string = "";
				
				for (int i = 0; i < WORD_VECTOR_LENGTH - 1; i++) {
					vector_string += ("" + vector_values[i].toString() + "\t");
				}
				vector_string += ("" + vector_values[WORD_VECTOR_LENGTH - 1].toString());
				
				output.write(key, new Text(vector_string));
			} catch (Exception e) {
				throw new IOException(e.getMessage() + ": " + curr);
				
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
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setJarByClass(NormalizeWordVector.class);
		job.setMapperClass(LoadVectorMapper.class);
		job.setReducerClass(AccumulateReducer.class);
		//jobsetNumReduceTasks(0);
		

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job,
				org.apache.hadoop.io.compress.BZip2Codec.class);

		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(),
				new NormalizeWordVector(), args);
		System.exit(res);
	}
}