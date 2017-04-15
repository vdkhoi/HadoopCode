package de.l3s.hadoop.mapreduce.feature.follow_up.URL;


import java.io.*;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
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

import java.util.PriorityQueue;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;

/*
 * This program aims to build some features which define in paper
 * 
 * The Whens and Hows of Learning to Rank for Web Search, page 601
 * 
 * Count Number of host of a domain appear in archive
 * 
 */


/*
Input

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
#25: Number of entities in anchors (NEW)
#26: Fraction of inlinks with query anchor (NEW)
#27: URLDomainDigit - Number of digit in domain - 54
#28: URLHostDigit - Number of digit in host - 55
#29: URLParts - Count of all parts in domain, path, and query string - 56
#30: URLQueryStringLength - Number of query string parameters - 58
#31: URLHostCharLength - 59
#32: URLPathCharLength - 60
#33: URLQueryStringCharLength - 61

Output

#1: domain
#2: number of host

*/


public class CountNumOfHostOfADomain extends Configured implements Tool {
	public static final int MAX_LENGTH = 10000;

	private static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text textKey = null;
		private Text textValue = null;
		
		private String extractDomain (String sURLAndPathAndQueryString) {
			String[] parts = sURLAndPathAndQueryString.split("\\)/");
			return parts[0];
		}
		
		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value != null) {
					String[] line = value.toString().split("\t");
					if (line.length == 2 && line[0].length() > 0) {
						String hostAndDomain = extractDomain(line[0]);
						int domainidx = hostAndDomain.lastIndexOf(',');
						String sHost = "", sDomain = "";
						if ((domainidx > 0) && (domainidx < hostAndDomain.length() - 1)) {
							sDomain = hostAndDomain.substring(0, domainidx);
							sHost = hostAndDomain.substring(domainidx + 1, hostAndDomain.length());
						}
						textKey = new Text(sDomain);
						textValue = new Text(sHost);
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


	private static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context output)
				throws IOException, InterruptedException {
			try {
				int count = 0;
				Iterator<Text> iter = values.iterator();
				while (iter.hasNext()) {
					iter.next();
					count ++;
				}
				output.write(key, new Text("" + count));
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
		// conf.setInputFormat(TextInputFormat.class);
		// conf.setOutputFormat(TextOutputFormat.class);

		Job job = Job.getInstance(conf, this.getClass().getName());

		job.setJobName(this.getClass().getName() + ".jar");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setJarByClass(CountNumOfHostOfADomain.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job,
				org.apache.hadoop.io.compress.BZip2Codec.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new CountNumOfHostOfADomain(), args);
		System.exit(res);
	}
}