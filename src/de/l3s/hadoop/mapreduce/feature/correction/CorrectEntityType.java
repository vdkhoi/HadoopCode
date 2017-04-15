package de.l3s.hadoop.mapreduce.feature.correction;

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

public class CorrectEntityType extends Configured implements Tool {
	
	public static int WORD_VECTOR_LENGTH = 100;

	private static class LoadDocumentWithEntityTypeMapper extends
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
				String[] line = row.split("[\t ]");
				output.write(new LongWritable(Long.parseLong(line[0])), new Text(line[line.length - 1]));
			} catch (Exception e) {
				throw new IOException(value.toString(), e);
			}
		}
	}

	private static class LoadResultToMergeMapper extends
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
				String[] line = row.split("\t");
				row = "";
				for (int i = 1; i < line.length; i ++) {
					if (line[i].length() > 0)
						row += (line[i] + "\t");
				}
				
				if (row.lastIndexOf('\t') == row.length() - 1)  {
					row = row.substring(0, row.length() -1);
				}
				
				output.write(new LongWritable(Long.parseLong(line[0])), new Text(row));
			} catch (Exception e) {
				throw new IOException(value.toString(), e);
			}
		}
	}
	
	public static class JoinReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,
				Context output) throws IOException, InterruptedException {
			
			Iterator<Text> iter = values.iterator();
			String curr = "", row = "", entity_type = "";
			
			while (iter.hasNext()) {
				curr = iter.next().toString();
				if (curr.split("[\t ]").length == 1) {
					entity_type = curr;
				}
				else {
					row = curr;
				}
			}
			
			output.write(key, new Text(row + "\t" + entity_type));
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

		job.setJarByClass(CorrectEntityType.class);
		job.setReducerClass(JoinReducer.class);
		//job.setMapperClass(VectorMapper.class);
		//jobsetNumReduceTasks(0);
		

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job,
				org.apache.hadoop.io.compress.BZip2Codec.class);

		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, LoadDocumentWithEntityTypeMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, LoadResultToMergeMapper.class);
		
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(),
				new CorrectEntityType(), args);
		System.exit(res);
	}
}