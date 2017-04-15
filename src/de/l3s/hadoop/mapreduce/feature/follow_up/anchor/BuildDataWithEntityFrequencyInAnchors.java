package de.l3s.hadoop.mapreduce.feature.follow_up.anchor;

import java.io.*;
import java.util.regex.Pattern;
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


*/
	
	
	
	
/* Output 	
	
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
#26: Number of entities in anchors (NEW)
#27: Fraction of inlinks with query anchor (NEW)
*/


public class BuildDataWithEntityFrequencyInAnchors extends Configured implements Tool {
		
	private static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		private String[] entity_list;
		
		@Override
	    public void setup(Context context) throws IOException {
	      Configuration conf = context.getConfiguration();
	      entity_list = conf.get("entity_list").split("\t");
	    }
		
		public static NullWritable NULL = NullWritable.get(); 
		
		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {

				if (value == null) return;
				String row = value.toString();
				if (row == null) return;
				String[] line = row.split("\t");

				GermanAnalyzer analyzer = new GermanAnalyzer();
				TokenStream tokens = analyzer.tokenStream(null, line[2]);
				CharTermAttribute charTermAttrib = tokens
						.getAttribute(CharTermAttribute.class);
				tokens.reset();
				String terms = "";
				while (tokens.incrementToken()) {
					String term = charTermAttrib.toString();
					if (term.length() > 2 && !term.matches("^[-+]?\\d+([\\.\\,]\\d+)*$")) {
						terms += (term + " ");
					}
				}
				
				tokens.end();
				tokens.close();
				analyzer.close();

				
				int count = 0;
				if (terms.length() > 3) {
					for (int i = 0; i < entity_list.length; i++) {
						if (terms.length() > entity_list[i].length()) {
							if (terms.indexOf(entity_list[i]) > 0) {
								count++;
								terms.replaceAll(Pattern.quote(entity_list[i]),
										"");
								if (terms.length() < 3)
									break;
							}
						}
					}
				}
				Double fraction;
				if (line[4].length() > 0 && line[18].length() > 0) {
					fraction = Double.parseDouble(line[4]) / Double.parseDouble(line[18]);
				}
				else
				{
					fraction = new Double(0.0f);
				}
				
				row = ( line[0] + "\t" +
						line[1] + "\t" +
						line[2] + "\t" +
						line[3] + "\t" +
						line[4] + "\t" +
						line[5] + "\t" +
						line[6] + "\t" +
						line[7] + "\t" +
						line[8] + "\t" +
						line[9] + "\t" +
						line[10] + "\t" +
						line[11] + "\t" +
						line[12] + "\t" +
						line[13] + "\t" +
						line[14] + "\t" +
						line[15] + "\t" +
						line[16] + "\t" +
						line[17] + "\t" +
						line[18] + "\t" +
						line[19] + "\t" +
						line[20] + "\t" +
						line[21] + "\t" +
						line[22] + "\t" +
						line[23] + "\t" +
						line[24] + "\t" +
						count    + "\t" + 
						fraction);
				output.write(new Text(row), NULL);
				
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
		File r = new File(args[2]);
		BufferedReader br = new BufferedReader(new FileReader(r));
		conf.set("entity_list", br.readLine());
		
		Job job = Job.getInstance(conf, this.getClass().getName());
		
		job.setJobName(this.getClass().getName() + ".jar");
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		
		job.setJarByClass(BuildDataWithEntityFrequencyInAnchors.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		
		FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.BZip2Codec.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new BuildDataWithEntityFrequencyInAnchors(), args);
		System.exit(res);

	}
}