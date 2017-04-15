package de.l3s.hadoop.mapreduce.feature.LETOR;

import java.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class BuildLETORFor15EntityBingScoringResult extends Configured implements Tool {
	
	/*
	 * Input format:
		1.	doc_id,
		2.	dest_url,
		3.	inlink, 
		4.	url_depth, 
		5.	query_string, 
		6.	search_word, 
		7.	query_in_url, 
		8.	news_url, 
		9.	wikipedia_url, 
		10.	core_pagerank, 
		11.	domain_pagerank, 
		12.	query_freq, 
		13.	anchor_time_spans, 
		14.	lucence_tf, 
		15.	doc_len, 
		16.	field_norm, 
		17.	lucene_idf, 
		18.	revision, 
		19.	revduration, 
		20.	domain_size, 
		21.	entity_type
	 * 
	 * 
	 * Output format
	 * 1. PageRank core 10 
	 * 2. PageRank domain 11
	 * 3. Inlink 3
	 * 4. Fraction of inlinks with query anchor 3 / 12 
	 * 5. Searching words in URL (suche, suchergebnis, suchen, query,suchwort) 6
	 * 6. Query frequency 12
	 * 7. Entity type 21
	 * 8. Max tf 14
	 * 9. Max idf 17
	 * 10. Doclen 15
	 * 11. Field norm 16
	 * 12. Query in URL (domain --> 2, url --> 1, not in --> 0) 6 (Require modification) 7
	 * 13. Query term in URL (domain --> 2, url --> 1, not in --> 0) 19
	 * 14. URL in Wikipedia of entity 9
	 * 15. Depth of URL 4
	 * 16. URL in news 8
	 * 17. Authority of URL in entity's topic 20
	 * 18. Query string ? 5
	 * 19. No. of revisions 18
	 * 20. Time series of query anchor 13
	 * 
	 * 
	 * 9 10 2 2/11 5 11 20 13 16 14 15 6 18 8 3 7 19 4 17 12
	 */
	
	private static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		private String qid = "";
		private String query = "";
		private String mode = "";
		
		@Override
	    public void setup(Context context) throws IOException {
	      Configuration conf = context.getConfiguration();
	      qid = conf.get("qid");
	      query = conf.get("query");
	      mode = conf.get("mode");
	    }
		
		public static NullWritable NULL = NullWritable.get(); 
		
		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				String row = value.toString();
				if (row == null) return;
				String[] line = row.split("\t");
				
				
				if (mode.equals("learn") && line.length != 22) return;
				if (mode.equals("score") && line.length != 21) return;
				if (line[5].length() == 0) return;
				
				int i = 0;
				double feature_4 = Double.parseDouble(line[i + 11]) / Double.parseDouble(line[i + 2]);
				
				for (int j = i + 2; j < line.length; j ++) {
					if (line[j].length() == 0) {
						line[j] = "0.0";
					}
				}
				
				if (mode.equals("score")) {
					if (line[0].length() == 0) {
						line[0] = "0.0";
					}
				}
				
				String letor_line = "";
				
				if (mode.equals("score")) {
					i = 0;
					letor_line = "0.0 qid:" + qid + 
							" 1:" + line[i + 9] + " 2:" + line[i + 10] + 
							" 3:" + line[i + 2] + " 4:" + feature_4 + 
							" 5:" + line[i + 5] + " 6:" + line[i + 11] + 
							" 7:" + line[i + 20] + " 8:" + line[i + 13] + 
							" 9:" + line[i + 16] + " 10:" + line[i + 14] + 
							" 11:" + line[i + 15] + " 12:" + line[i + 6] +
							" 13:" + line[i + 18] + " 14:" + line[i + 8] +
							" 15:" + line[i + 3] + " 16:" + line[i + 7] + 
							" 17:" + line[i + 19] + " 18:" + line[i + 4] + 
							" 19:" + line[i + 17] + " 20:" + line[i + 12] +
							" #query:" + query + "|doc_id:" + line[i + 0] + 
							"|dest_url:" + line[i + 1];
				}
				
				
				
				if (mode.equals("learn")) {
					i = 1;
					letor_line = line[0] + " qid:" + qid + 
							" 1:" + line[i + 9] + " 2:" + line[i + 10] + 
							" 3:" + line[i + 2] + " 4:" + feature_4 + 
							" 5:" + line[i + 5] + " 6:" + line[i + 11] + 
							" 7:" + line[i + 20] + " 8:" + line[i + 13] + 
							" 9:" + line[i + 16] + " 10:" + line[i + 14] + 
							" 11:" + line[i + 15] + " 12:" + line[i + 6] +
							" 13:" + line[i + 18] + " 14:" + line[i + 8] +
							" 15:" + line[i + 3] + " 16:" + line[i + 7] + 
							" 17:" + line[i + 19] + " 18:" + line[i + 4] + 
							" 19:" + line[i + 17] + " 20:" + line[i + 12] +
							" #query:" + query + "|doc_id:" + line[i + 0] + 
							"|dest_url:" + line[i + 1];
				}
				
				output.write(new Text(letor_line), NULL);
				
			} catch (Exception e) {
				throw new IOException(value.toString(), e);
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		conf.setBoolean("map.output.compress", true);
		conf.set("mapreduce.output.compression.type", "BLOCK");
		conf.setClass("mapreduce.output.fileoutputformat.compress.codec", org.apache.hadoop.io.compress.BZip2Codec.class, org.apache.hadoop.io.compress.CompressionCodec.class);
		conf.set("mapreduce.task.timeout", "1800000");
		conf.set("mapreduce.map.java.opts", "-Xmx3072m");
		conf.set("mapreduce.child.java.opts", "-Xmx3072m");
		conf.set("qid", args[2]);
		conf.set("query", args[3]);
		conf.set("mode", args[4]);
        //conf.setInputFormat(TextInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);
		
		Job job = Job.getInstance(conf, "BuildLETORFor15EntityScoringResult");
		
		job.setJobName(this.getClass().getName() + ".jar");
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		
		job.setJarByClass(BuildLETORFor15EntityBingScoringResult.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		//job.setCombinerClass(Reduce.class);
		//job.setReducerClass(Reduce.class);
		
		FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.BZip2Codec.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new BuildLETORFor15EntityBingScoringResult(), args);
		System.exit(res);

	}
}