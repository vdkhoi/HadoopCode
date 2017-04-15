package de.l3s.hadoop.mapreduce.feature.word2vec.apply;

import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.*;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;


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

public class BuildWord2VecFeatureFromAnchors extends Configured implements Tool {
	
	public static int WORD_VECTOR_LENGTH = 100;

	private static class TokenizerMapper extends
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
				String[] line = row.split("\t");
				TObjectIntHashMap<String> term_set = new TObjectIntHashMap<String>();
				GermanAnalyzer analyzer = new GermanAnalyzer();
				TokenStream tokens = analyzer.tokenStream(null, line[2]);
				CharTermAttribute charTermAttrib = tokens
						.getAttribute(CharTermAttribute.class);
				tokens.reset();
				while (tokens.incrementToken()) {
					String term = charTermAttrib.toString();
					if (term.length() > 2
							&& !term.matches("^[-+]?\\d+([\\.\\,]\\d+)*$")) {
						term_set.adjustOrPutValue(term, 1, 1);
					}
				}

				tokens.end();
				tokens.close();
				analyzer.close();
				
				int total_terms = term_set.size();
				
				for (String t : term_set.keySet()) {
					int arg1 = term_set.get(t);
					float percentage = (float) arg1 / (float) total_terms;
					output.write(new Text(t), new Text(line[0] + "\t" + percentage));
					// term TAB | doc_id TAB percent
				}
				
			} catch (Exception e) {
				throw new IOException(value.toString(), e);
			}
		}
	}

	private static class VectorMapper extends
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
				String[] line = row.split(" ", 2);
				
				output.write(new Text(line[0]), new Text(line[1]));
				
				// term TAB | vector
			} catch (Exception e) {
				throw new IOException(value.toString(), e);
			}
		}
	}
	
	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context output) throws IOException, InterruptedException {
			
			Iterator<Text> iter = values.iterator();
			TLongFloatHashMap row_set = new TLongFloatHashMap();
			String curr;
			String[] line = null; 
			boolean isVector = false;
			String vector_string = "";
			
			long doc_id = 0;
			float percentage = 0.0f;
			
			while (iter.hasNext()) {
				curr = iter.next().toString();
				line = curr.split("[\t ]", 2);

				try {
					if (line[0].length() > 0) {
						doc_id = Long.parseLong(line[0]);
						//No exception: this record is doc_id
						percentage = Float.parseFloat(line[1]);
						row_set.put(doc_id, percentage);
					}
				}
				catch (NumberFormatException ex) {
					if (isVector) continue;
					isVector = true;
					vector_string = curr;
				}
				
			}
			
			
			
			if (isVector) {
				long[] doc_id_arr = row_set.keys();
				for(int j = 0; j < doc_id_arr.length; j ++) {
					percentage = row_set.get(doc_id_arr[j]);
					output.write(new Text("" + doc_id_arr[j] + "\t" + percentage), new Text(vector_string));
				}
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

		job.setJarByClass(BuildWord2VecFeatureFromAnchors.class);
		job.setReducerClass(JoinReducer.class);
		//job.setMapperClass(VectorMapper.class);
		//jobsetNumReduceTasks(0);
		

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job,
				org.apache.hadoop.io.compress.BZip2Codec.class);

		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, TokenizerMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, VectorMapper.class);

		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new BuildWord2VecFeatureFromAnchors(), args);
		System.exit(res);
	}
}