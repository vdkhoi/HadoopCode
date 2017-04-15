package de.l3s.hadoop.mapreduce.feature.follow_up.URL;

import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.*;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
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
import org.apache.uima.pear.util.StringUtil;

/*
 * This program aims to build some features which define in paper
 * 
 * The Whens and Hows of Learning to Rank for Web Search, page 601
 * 
 * 
 * Input
 * 
 *  	

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

 Output


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
 #30: URLDomainHost - Count of host of a domain - 57
 #31: URLQueryStringLength - Number of query string parameters - 58
 #32: URLHostCharLength - 59
 #33: URLPathCharLength - 60
 #34: URLQueryStringCharLength - 61

 */

public class CountURLSimpleFeatureFromReference extends Configured implements Tool {
	public static final int MAX_LENGTH = 10000;

	private static class Map extends
			Mapper<LongWritable, Text, Text, NullWritable> {

		private static NullWritable NULL = NullWritable.get();
		TObjectIntHashMap<String> domainHostMap = new TObjectIntHashMap<String>();

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			String[] pair_string = conf.get("host_in_domain").split("\t");
			for (int i = 0; i < pair_string.length / 2; i++) {
				domainHostMap.put(pair_string[2 * i],
						Integer.parseInt(pair_string[2 * i + 1]));
			}
		}

		private String extractHostAndDomain(String sURLAndPathAndQueryString) {
			String[] parts = sURLAndPathAndQueryString.split("\\)/");
			return parts[0];
		}
		
		
		private int computeFeature15(String sURLAndPathAndQueryString) {
			/*
			 * Count URL Depth
			 */


			return StringUtil.countWords("/");
		}

		private int computeFeature54(String hostAndDomain) {
			/*
			 * Count number of digit in domain of URL
			 */

			int domainidx = hostAndDomain.lastIndexOf(','), result = 0;
			String sDomain = "";

			if (domainidx > 0) {
				sDomain = hostAndDomain.substring(0, domainidx);
			}

			for (int j = 0, len = sDomain.length(); j < len; j++) {
				if (Character.isDigit(sDomain.charAt(j))) {
					result++;
				}
			}

			return result;
		}

		private int computeFeature55(String hostAndDomain) {
			/*
			 * Count number of digit in host name of URL
			 */

			int domainidx = hostAndDomain.lastIndexOf(','), result = 0;
			String sHost = "";

			if (domainidx > 0) {
				sHost = hostAndDomain.substring(domainidx + 1,
						hostAndDomain.length());
			}

			for (int j = 0, len = sHost.length(); j < len; j++) {
				if (Character.isDigit(sHost.charAt(j))) {
					result++;
				}
			}

			return result;
		}

		private int computeFeature56(String sURLAndPathAndQueryString) {
			/*
			 * Count number of parts in domain, path, and query string
			 */

			String sDomain = extractHostAndDomain(sURLAndPathAndQueryString);

			int countOfPartInDomain = StringUtils.countMatches(sDomain, ",");
			int countOfPartInPath = StringUtils.countMatches(
					sURLAndPathAndQueryString, "/");
			int countOfPartInQueryString = StringUtils.countMatches(
					sURLAndPathAndQueryString, "&");

			return countOfPartInDomain + countOfPartInPath
					+ countOfPartInQueryString;
		}

		private int computeFeature57(String hostAndDomain) {
			/*
			 * Count number of parts in query string
			 */

			int domainidx = hostAndDomain.lastIndexOf(','), result = 0;
			String sDomain = "";

			if (domainidx > 0) {
				sDomain = hostAndDomain.substring(0, domainidx);
			}

			result = domainHostMap.get(sDomain);

			return result;
		}

		private int computeFeature58(String sURLAndPathAndQueryString) {
			/*
			 * Count number of parts in query string
			 */

			int countOfPartInQueryString = StringUtils.countMatches(
					sURLAndPathAndQueryString, "&");

			return countOfPartInQueryString;
		}

		private int computeFeature59(String hostAndDomain) {
			/*
			 * Compute host length by character
			 */

			int domainidx = hostAndDomain.lastIndexOf(',');
			String sHost = "";

			if (domainidx > 0) {
				sHost = hostAndDomain.substring(domainidx + 1,
						hostAndDomain.length());
			}

			return sHost.length();
		}

		private int computeFeature60(String sURLAndPathAndQueryString) {
			/*
			 * Compute path length by character
			 */

			String sPath = sURLAndPathAndQueryString.substring(
					sURLAndPathAndQueryString.indexOf('/') + 1,
					sURLAndPathAndQueryString.lastIndexOf('/'));

			return sPath.length();
		}

		private int computeFeature61(String sURLAndPathAndQueryString) {
			/*
			 * Compute query string length by character
			 */

			String sQuery = sURLAndPathAndQueryString.substring(
					sURLAndPathAndQueryString.indexOf('?') + 1,
					sURLAndPathAndQueryString.length());

			return sQuery.length();
		}

		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value != null) {
					String row = value.toString();
					String[] line = row.split("\t", 2);
					if (line.length > 1) {
						String url = line[0];
						String sHostAndDomain = extractHostAndDomain(url);
						Text result = new Text(row 					+ "\t"
								+ computeFeature15(url) 			+ "\t"
								+ computeFeature54(sHostAndDomain) 	+ "\t"  //URL feature
								+ computeFeature55(sHostAndDomain) 	+ "\t"  //URL feature
								+ computeFeature56(url) 			+ "\t"  //URL feature
								+ computeFeature57(sHostAndDomain) 	+ "\t"  //URL feature
								+ computeFeature58(url) 			+ "\t"  //URL feature
								+ computeFeature59(sHostAndDomain) 	+ "\t"  //URL feature
								+ computeFeature60(url) 			+ "\t"  //URL feature
								+ computeFeature61(url));              		//URL feature
						output.write(result, NULL);
					}
				}
			} catch (IOException e) {
				throw new IOException("Error here: " + value.toString(), e);
			} catch (InterruptedException e1) {
				throw new InterruptedException("Error here: "
						+ value.toString() + "\r\n" + e1);
			}

			catch (Exception e2) {
				throw new IOException("Error here: " + value.toString(), e2);
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
		conf.set("host_in_domain", loadFile(args[2]));
		// conf.setInputFormat(TextInputFormat.class);
		// conf.setOutputFormat(TextOutputFormat.class);

		Job job = Job.getInstance(conf, this.getClass().getName());

		job.setJobName(this.getClass().getName() + ".jar");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setJarByClass(CountURLSimpleFeatureFromReference.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job,
				org.apache.hadoop.io.compress.BZip2Codec.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public String loadFile(String filePath) {
		File f = new File(filePath);
		String result = "", rl = "";
		try {
			BufferedReader br = new BufferedReader(new FileReader(f));
			while ((rl = br.readLine()) != null) {
				result += (rl + "\t");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new CountURLSimpleFeatureFromReference(), args);
		System.exit(res);
	}
}