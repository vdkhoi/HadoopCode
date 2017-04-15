package de.l3s.hadoop.mapreduce;

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

/* Input

#1: doc_id
#2: dest_url
#3: anchors
#4: inlink
#5: avg_inlink
#6: outlink
#7: avg_outlink
#8: similarity
#9: entity_in_last_part_of_url
#10: url_depth
#11: query_string
#12: search_word
#13: query_in_url
#14: news_url
#15: wikipedia_url
#16: core_pagerank
#17: domain_pagerank
#18: query_freq
#19: anchor_time_spans
#20: doc_len
#21: revision
#22: revduration
#23: domain_size
#24: entity_type
#25: Number of entities in anchors
#26: Fraction of inlinks with query anchor


*/
	
	
	
	
/* Output 	
	
#1: doc_id
#2: dest_url
#3: anchors
#4: query_entity  (NEW)
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


public class AddQueryIdIntoResultSet extends Configured implements Tool {
		
	private static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		private String entity;
		
		@Override
	    public void setup(Context context) throws IOException {
	      Configuration conf = context.getConfiguration();
	      entity = conf.get("entity");
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
						
				row = ( line[0] + "\t" +
						line[1] + "\t" +
						line[2] + "\t" +
						entity  + "\t");
				
				for (int i = 3; i < line.length - 1; i ++) {
					row += (line[i] + "\t");
				}
				row += (line[line.length - 1]);
				
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
		conf.set("entity", args[2]);
		
		Job job = Job.getInstance(conf, this.getClass().getName());
		
		job.setJobName(this.getClass().getName() + ".jar");
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		
		job.setJarByClass(AddQueryIdIntoResultSet.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		
		FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.BZip2Codec.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new AddQueryIdIntoResultSet(), args);
		System.exit(res);

	}
}