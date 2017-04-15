package de.l3s.hadoop.mapreduce.index.archive.recover;

// Feature 3, 5, 15

import gnu.trove.map.hash.TObjectIntHashMap;
import java.io.*;
import java.util.HashSet;
import java.util.Iterator;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.uima.pear.util.StringUtil;

class DocumentVector {
    TObjectIntHashMap<String> wordMap = new TObjectIntHashMap<String>();

    public void incCount(String word) {
        Integer oldCount = wordMap.get(word);
        wordMap.put(word, oldCount == null ? 1 : oldCount + 1);
    }

    float getCosineSimilarityWith(DocumentVector otherVector) {
        float innerProduct = 0;
        for(String w: this.wordMap.keySet()) {
            innerProduct += this.getCount(w) * otherVector.getCount(w);
        }
        return (float)innerProduct / (this.getNorm() * otherVector.getNorm());
    }

    float getNorm() {
        float sum = 0;
        for (Integer count : wordMap.values()) {
            sum += count * count;
        }
        return (float)Math.sqrt(sum);
    }

    int getCount(String word) {
        return wordMap.containsKey(word) ? wordMap.get(word) : 0;
    }

    public DocumentVector(String doc) {
        for(String w:doc.split("[^a-zA-Z]+")) {
            incCount(w);
        }
    }

}



public class MergeIndexDataWithURLFeatures extends Configured implements Tool {
	public static final int DATA_MAP_1 = 1;
	public static final int DATA_MAP_2 = 2;
	public static final int DATA_MAP_3 = 3;
	public static final String FIELD_TO_APPEND = "field_to_append";
	public static final String HOST_IN_DOMAIN = "host_in_domain";
	public static final String URL_IS_NEWS_DOMAIN = "url_is_news_domain";
	public static final String URL_IS_WIKI_REFERENCE = "url_is_wiki_reference";
	
	public static int field_to_append = 0;
	
	
	public static TObjectIntHashMap<String> domainHostMap = new TObjectIntHashMap<String>();
	public static HashSet<String> news_domains = new HashSet<String>();
	public static HashSet<String> wiki_references = new HashSet<String>();

	

	private static class LoadDataMap extends Mapper<LongWritable, Text, Text, Text> {
		private Text textKey = null;
		private Text textValue = null;
		
		
		private Logger logger = Logger.getLogger(LoadDataMap.class);
		
		@Override
		protected void setup(Context context) throws IOException ,InterruptedException {
			Configuration conf = context.getConfiguration();
			field_to_append = conf.getInt(FIELD_TO_APPEND, 1);
			String rl = "";
			String[] line = null;
			
			logger.info("Loading hosts...");
			
			Path pt=new Path(conf.get(HOST_IN_DOMAIN));//Location of file in HDFS
	        FileSystem fs = FileSystem.get(conf);
	        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			while ((rl = br.readLine()) != null) {
				line = rl.split("\t");
				domainHostMap.put(line[0],
						Integer.parseInt(line[1]));
			}
			br.close();
			
			logger.info("Last sample: (" + line[0] +", " + line[1] + ")");
			logger.info("Loaded hosts...");
			
			logger.info("Loading news domain...");
			
			pt=new Path(conf.get(URL_IS_NEWS_DOMAIN));//Location of file in HDFS
	        fs = FileSystem.get(conf);
	        br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			while ((rl = br.readLine()) != null) {
				news_domains.add(rl);
			}
			br.close();
			
			logger.info("Last sample: (" + rl + ")");
			logger.info("Loaded news domain...");
			logger.info("Loading wiki references...");
			
			pt=new Path(conf.get(URL_IS_WIKI_REFERENCE));//Location of file in HDFS
	        fs = FileSystem.get(conf);
	        br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			while ((rl = br.readLine()) != null) {
				wiki_references.add(convert2SURTForm(rl));
			}
			br.close();
			
			logger.info("Last sample: (" + rl + ")");
			logger.info("Loaded wiki references...");

		}

		

		// IN: domain_ID TAB Core_URL_ID TAB URL || TAB URL
		// OUT: <domain_ID , Core_URL_ID TAB URL>
		
		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value != null) {
					String[] line = value.toString().split("\t", 2);

					if (line.length > 1 && line[0].length() > 0) {
						textKey = new Text(line[0]);
						textValue = new Text(DATA_MAP_1 + ":" + line[1]);
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
	
	
	private static class LoadIndexListMap extends Mapper<LongWritable, Text, Text, Text> {
		private Text textKey = null;
		private Text textValue = null;

		// IN: Domain_ID TAB PR_Score

		
		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value != null) {
					String[] line = value.toString().split("\t", 2);

					if (line.length > 1) {
						textKey = new Text(line[0]);
						textValue = new Text(DATA_MAP_2 + ":" + line[1]);
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
	
	private static class LoadDomainSizeMap extends Mapper<LongWritable, Text, Text, Text> {
		private Text textKey = null;
		private Text textValue = null;

		// IN: Domain_ID TAB PR_Score
		

		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				if (value != null) {
					String[] line = value.toString().split("\t", 2);

					if (line.length > 1) {
						textKey = new Text(line[0]);
						textValue = new Text(DATA_MAP_3 + ":" + line[1]);
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
	

	private static class Reduce extends Reducer<Text, Text, Text, NullWritable> {

		private String extractHostAndDomain(String sURLAndPathAndQueryString) {
			String[] parts = sURLAndPathAndQueryString.split("\\)/");
			return parts[0];
		}
		
		public int computeFeature5(String url) {
			return (url.indexOf("suche") > 4 || url.indexOf("suche?") > 4
					|| url.indexOf("suchen") > 4
					|| url.indexOf("suchen?") > 4
					|| url.indexOf("suchergebnis") > 4
					|| url.indexOf("suchergebnis") > 4 || url
					.indexOf("query=") > 4) ? 1 : 0;
		}
		
		private int computeFeature14(String hostAndDomain) {
			/*
			 * Count URL in Wiki references
			 */
			for (String domain : wiki_references) {
				if ((hostAndDomain + ")/").startsWith(domain))
					return 1;
			}

			return 0;
		}
		
		private int computeFeature15(String sURLAndPathAndQueryString) {
			/*
			 * Count URL Depth
			 */


			return StringUtil.countWords("/");
		}
		
		private int computeFeature16(String hostAndDomain) {
			/*
			 * Count URL in News Domains
			 */

			for (String domain : news_domains) {
				if ((hostAndDomain + ")/").startsWith(domain))
					return 1;
			}

			return 0;
		}
		
		private int computeFeature18(String url) {
			/*
			 * Count URL in News Domains
			 */
			if (url.indexOf('?') > 0)
				return 1;
			else
				return 0;
		}
		
		private float computeFeature23(String url, String anchors) {
			/*
			 * Similarity URL with anchors
			 */
			
			if (url.length() == 0) return 0.0f;
			if (anchors.length() == 0) return 0.0f;
			
			DocumentVector v1 = new DocumentVector(url.toString());
			DocumentVector v2 = new DocumentVector(anchors.toString());
			return v1.getCosineSimilarityWith(v2);
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
			
			int first_appear = sURLAndPathAndQueryString.indexOf('/');
			int last_appear  = sURLAndPathAndQueryString.lastIndexOf('/');
			
			if (first_appear > 0 && last_appear > first_appear) {
				String sPath = sURLAndPathAndQueryString.substring(first_appear + 1,
														last_appear);
				return sPath.length();
			}
			else
				return 0;
		}

		private int computeFeature61(String sURLAndPathAndQueryString) {
			/*
			 * Compute query string length by character
			 */

			int question_appear = sURLAndPathAndQueryString.indexOf('?');
			
			if (question_appear > 0) {
				String sQuery = sURLAndPathAndQueryString.substring(
					sURLAndPathAndQueryString.indexOf('?') + 1,
					sURLAndPathAndQueryString.length());
				return sQuery.length();
			}
			else
				return 0;
		}
		
		private String computeURLFeatures (String url, String anchors, String feature17) {
			String sHostAndDomain = extractHostAndDomain(url);
			String url_features	= 
					  computeFeature5(url)				+ "\t"	//URL feature
					+ computeFeature14(sHostAndDomain)	+ "\t"	//URL feature
					+ computeFeature15(url)				+ "\t"	//URL feature
					+ computeFeature16(sHostAndDomain)	+ "\t"	//URL feature
					+ feature17							+ "\t"	//URL feature
					+ computeFeature18(url)				+ "\t"	//URL feature
					+ computeFeature23(url, anchors)	+ "\t"	//URL feature
					+ computeFeature54(sHostAndDomain) 	+ "\t"  //URL feature
					+ computeFeature55(sHostAndDomain) 	+ "\t"  //URL feature
					+ computeFeature56(url) 			+ "\t"  //URL feature
					+ computeFeature57(sHostAndDomain) 	+ "\t"  //URL feature
					+ computeFeature58(url) 			+ "\t"  //URL feature
					+ computeFeature59(sHostAndDomain) 	+ "\t"  //URL feature
					+ computeFeature60(url) 			+ "\t"  //URL feature
					+ computeFeature61(url);	           		//URL feature
			return url_features;
		}

		
		
		public static final NullWritable NULL = NullWritable.get();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context output)
				throws IOException, InterruptedException {
			try {
				Iterator<Text> iter = values.iterator();
				String current, score_pagerank = "n/a\tn/a", feature17_domain_size = "1", url = new String(key.toString()), url_features = "";
				
				HashSet<String> data_rows = new HashSet<String>();
				boolean hasPageRankScore = false;
				boolean hasDomainSize = false;
				
				while (iter.hasNext()) {
					current = iter.next().toString();
					int id = Integer.parseInt(current.substring(0, 1));
					current = current.substring(2, current.length());
					switch (id){
						case DATA_MAP_1: {
							if (hasPageRankScore && hasDomainSize) {
								url_features = computeURLFeatures(url, current.split("\t", 2)[0], feature17_domain_size);
								output.write(new Text(key.toString() + "\t" + current + "\t" + score_pagerank + "\t" + url_features), NULL);
							}	
							else {
								data_rows.add(current);
							}
							break;
						}
						case DATA_MAP_2: {
							score_pagerank = current;
							hasPageRankScore = true;
							break;
						}
						case DATA_MAP_3: {
							feature17_domain_size = current;
							hasDomainSize = true;
							break;
						}

					}
					
				}
				
			// Process data before seeing pagerank and domain size
				
				for (String dat : data_rows) {
					url_features = computeURLFeatures(url, dat.split("\t", 2)[0], feature17_domain_size);
					output.write(new Text(key.toString() + "\t" + dat + "\t" + score_pagerank + "\t" + url_features), NULL);
				}
			
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
		conf.setInt(FIELD_TO_APPEND, Integer.parseInt(args[4]));
		conf.set(HOST_IN_DOMAIN, args[5]);
		conf.set(URL_IS_NEWS_DOMAIN, args[6]);
		conf.set(URL_IS_WIKI_REFERENCE, args[7]);

		Job job = Job.getInstance(conf, this.getClass().getName());

		job.setJobName(this.getClass().getName() + ".jar");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		

		job.setJarByClass(MergeIndexDataWithURLFeatures.class);
		//job.setMapperClass(LoadDataMap.class);
		//job.setCombinerClass(Reduce.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, LoadDataMap.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, LoadIndexListMap.class);
		MultipleInputs.addInputPath(job, new Path(args[2]),
				TextInputFormat.class, LoadDomainSizeMap.class);
		
		job.setReducerClass(Reduce.class);

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job,
				org.apache.hadoop.io.compress.BZip2Codec.class);

		
		
		
		
		
		
//		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static String convert2SURTForm(String dest_url) {
		
		String decodeURL = dest_url;
		
		if (decodeURL.indexOf("http://www.") == 0)
			decodeURL = decodeURL.replaceFirst(
					Pattern.quote("http://www."), "");

		if (decodeURL.indexOf("https://www.") == 0)
			decodeURL = decodeURL.replaceFirst(
					Pattern.quote("https://www."), "");

		if (decodeURL.indexOf("http://") == 0)
			decodeURL = decodeURL.replaceFirst(
					Pattern.quote("http://"), "");

		if (decodeURL.indexOf("https://") == 0)
			decodeURL = decodeURL.replaceFirst(
					Pattern.quote("https://"), "");
		
		if (decodeURL.indexOf("www.") == 0)
			decodeURL = decodeURL.replaceFirst(
				Pattern.quote("www."), "");
		
    	String revfirst = "";
    	
    	
		String[] dn = decodeURL.split(Pattern.quote("."));
		int len = dn.length;
		revfirst = dn[0];
		for (int i = 1; i < len; i++) {
			revfirst = dn[i] + "," + revfirst;
		}
        return (revfirst + ")/").toLowerCase();
    }


	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new MergeIndexDataWithURLFeatures(), args);
		System.exit(res);
	}
}