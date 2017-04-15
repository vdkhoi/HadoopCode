package de.l3s.hadoop.mapreduce.ranking;

import java.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;


public class BM25Okapi_Ranking extends Configured implements Tool {
	
	public static long N = 23375547635L;
	
	public static long n = 0;  // Change in run-time
	
	public static double IDF = 0.0D;
	
	public static double k1 = 1.2D;
	
	public static double b = 0.75D;
	
	public static double avgdl = 50.0D;
	
	public static int query_len = 0; // Change in run-time
	
	public static String query = "";
	
	public static long calculate_n(String path) {
		try {
			File fs = new File(path);
			BufferedReader br = new BufferedReader(new FileReader(fs));
			String line = "";
			line = br.readLine();
			return Long.parseLong(line);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0L;
	}
	
	
	
	public static double score(int doc_len) {
		double value = 0.0D;
		value = query_len * IDF * (k1 + 1) / (k1 * (1 - b + b * (doc_len / avgdl)));
		return value;		
	}
	
	private static class TFMap extends Mapper<LongWritable, Text, Text, LongWritable> {
		
		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				String[] fields = value.toString().split("\t");
				if (query.indexOf(fields[1]) >= 0)
					output.write(value, new LongWritable(Long.parseLong(fields[2])));		
			} catch (IOException e) {
				throw new IOException(value.toString(), e);
			}
			catch (InterruptedException e1){
				throw e1;
			}
			
			catch (Exception e2){
				throw new IOException(value.toString(), e2);
			}
			
		}
	}

	
	private static class RankingMap extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		
		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				String[] fields = value.toString().split("\t");
				Tokenizer tokenizer = new StandardTokenizer(new StringReader(fields[1]));
				CharTermAttribute charTermAttrib = tokenizer.getAttribute(CharTermAttribute.class);
				tokenizer.reset();
				output.write(value, new DoubleWritable(score(charTermAttrib.length())));
				tokenizer.end();
				tokenizer.close();
				
			} catch (IOException e) {
				throw new IOException(value.toString(), e);
			}
			catch (InterruptedException e1){
				throw e1;
			}
			
			catch (Exception e2){
				throw new IOException(value.toString(), e2);
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
        //conf.setInputFormat(TextInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);
		
		Job job = Job.getInstance(conf, "BM25_Okapi");
		
		job.setJobName(this.getClass().getName() + ".jar");
		
		job.setJarByClass(BM25Okapi_Ranking.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		
		
		job.setMapperClass(RankingMap.class);
		//job.setCombinerClass(Reduce.class);
		//job.setReducerClass(Reduce.class);
		
		FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.BZip2Codec.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		n = calculate_n(args[1]);
		
		if (n == 0) 
			System.exit(0);
		
		query = args[2];
		
		Tokenizer tokenizer = new StandardTokenizer(new StringReader(query));
		CharTermAttribute charTermAttrib = tokenizer
				.getAttribute(CharTermAttribute.class);
		tokenizer.reset();
		query_len = charTermAttrib.length();
		tokenizer.end();
		tokenizer.close();
		
		IDF = Math.log((N - n + 0.5) / (n + 0.5));
		
		int res = ToolRunner.run(new Configuration(), new BM25Okapi_Ranking(), args);
		System.exit(res);

	}
}