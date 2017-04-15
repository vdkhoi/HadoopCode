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

 #1: doc_id
 #2: dest_url
 #3: anchors
 #4: query_entity
 #5: inlink
 #6: avg_inlink
 #7: outlink
 #8: avg_outlink
 #9: similarity
 #10: entity_in_last_part_of_url
 #11: url_depth
 #12: query_string
 #13: search_word
 #14: query_in_url
 #15: news_url
 #16: wikipedia_url
 #17: core_pagerank
 #18: domain_pagerank
 #19: query_freq
 #20: anchor_time_spans
 #21: doc_len
 #22: revision
 #23: revduration
 #24: domain_size
 #25: entity_type
 #26: Number of entities in anchors
 #27: Fraction of inlinks with query anchor
 */

/* Output

Repeat many times with a pair <doc_id, query_entity>
#1: doc_id
#2: query_entity
#3: WordVector value of a term
...
#102: WordVector value of a term
End repeat
*/

/*
 * Syntax
 * hadoop jar /home/khoi/jars/HadoopCode-0.0.1-SNAPSHOT-jar-with-dependencies.jar de.l3s.hadoop.mapred.feature.word2vec.apply.BuildWord2VecFeatureFromAnchors evaluation/200_result_es_enhanced_features_add_two_features_add_query/Bilderbuchmuseum evaluation/word2vec_vector temp_debug_wordvector 
 */

public class BuildWord2VecFeatureFromAnchorsStep2 extends Configured implements Tool {
	
	public static int WORD_VECTOR_LENGTH = 100;

	private static class LoadVectorMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		
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
				
				
				output.write(new Text(line[0]), new Text(line[1]));
				
			} catch (Exception e) {
				throw new IOException(value.toString(), e);
			}
		}
	}

	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
		
		
		private static BigDecimal[] extractVector(String strVec) {
			BigDecimal[] result = new BigDecimal[WORD_VECTOR_LENGTH];
			String[] line = strVec.split("[\t ]");
			for (int i = 0; i < WORD_VECTOR_LENGTH; i ++) {
				if (line[i + 1].length() > 0) {
					result[i] = new BigDecimal(line[i + 1]);
					result[i] = result[i].multiply(new BigDecimal(line[0]));
				}
				else {
					result[i] = new BigDecimal(0.0f);
				}
			}
			return result;
		}
		
		private static BigDecimal[] addTwoVector(BigDecimal[] v1, BigDecimal[] v2) {
			BigDecimal[] result = new BigDecimal[WORD_VECTOR_LENGTH];
			if ((v1.length != WORD_VECTOR_LENGTH) || (v2.length != WORD_VECTOR_LENGTH))
				return null;
			for (int i = 0; i < WORD_VECTOR_LENGTH; i ++) {
				result[i] = v1[i].add(v2[i]);
			}
			return result;
		}
		
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context output) throws IOException, InterruptedException {
			
			Iterator<Text> iter = values.iterator();
			
			String curr;
			String vector_string = "";
			BigDecimal[] result_vector = new BigDecimal[WORD_VECTOR_LENGTH];
			
			//Init values
			for (int i = 0; i < WORD_VECTOR_LENGTH; i++) {
				result_vector[i] = new BigDecimal(0.0f);
			}
			
			while (iter.hasNext()) {
				curr = iter.next().toString();
				result_vector = addTwoVector(result_vector, extractVector(curr));
			}
			
			for (int i = 0; i < WORD_VECTOR_LENGTH - 1; i++) {
				vector_string += (result_vector[i].toString() + "\t");
			}
			
			vector_string += (result_vector[WORD_VECTOR_LENGTH - 1].toString());
			
			output.write(key, new Text(vector_string));
	
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		conf.setBoolean("map.output.compress", true);
		conf.set("mapreduce.output.compression.type", "BLOCK");
		conf.setClass("mapreduce.output.fileoutputformat.compress.codec",
				org.apache.hadoop.io.compress.BZip2Codec.class,
				org.apache.hadoop.io.compress.CompressionCodec.class);
		conf.set("mapreduce.task.timeout", "21600000");
		conf.set("mapreduce.map.java.opts", "-Xmx3072m");
		conf.set("mapreduce.child.java.opts", "-Xmx3072m");
		Job job = Job.getInstance(conf, this.getClass().getName());
		job.setJobName(this.getClass().getName() + ".jar");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setJarByClass(BuildWord2VecFeatureFromAnchorsStep2.class);
		job.setReducerClass(JoinReducer.class);
		job.setMapperClass(LoadVectorMapper.class);
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
				new BuildWord2VecFeatureFromAnchorsStep2(), args);
		System.exit(res);
	}
}