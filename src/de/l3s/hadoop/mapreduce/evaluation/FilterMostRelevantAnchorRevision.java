package de.l3s.hadoop.mapreduce.evaluation;

import java.io.*;
import java.util.ArrayList;
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

import com.google.gson.Gson;

/*
 * Try to get the time of document which anchor first point to
 * 
 */


public class FilterMostRelevantAnchorRevision extends Configured implements Tool {

	private static class Map extends
			Mapper<LongWritable, Text, Text, NullWritable> {
		
		private String[] entityList; 
		
		public void setup(Context context) {
			Gson json = new Gson();
			entityList = json.fromJson(context.getConfiguration().get("entity.list"), String[].class);
		}
		

		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				
				if (value == null) return;
				String[] line = value.toString().split("\t");
				
				if (line.length == 4) {
					for (int i = 0; i < entityList.length; i ++){
						String[] terms = entityList[i].split(" ");
						for (int j = 0; j < terms.length; j ++) {
							if (line[0].toLowerCase().indexOf(terms[j]) >= 0 || line[2].toLowerCase().indexOf(terms[j]) >= 0) {
								output.write(value, NullWritable.get());
							}
						}
						
					}
				}
				
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
		
		entityFile = args[2];
		
		entityList = getSampleEntities(entityFile);
		
		Gson json = new Gson();
		
		
		Configuration conf = this.getConf();
		conf.setBoolean("map.output.compress", true);
		conf.set("mapreduce.output.compression.type", "BLOCK");
		conf.setClass("mapreduce.output.fileoutputformat.compress.codec",
				org.apache.hadoop.io.compress.BZip2Codec.class,
				org.apache.hadoop.io.compress.CompressionCodec.class);
		conf.set("mapreduce.task.timeout", "1800000");
		conf.set("mapreduce.map.java.opts", "-Xmx3072m");
		conf.set("mapreduce.child.java.opts", "-Xmx3072m");
		conf.set("entity.list", json.toJson(entityList));
		

		Job job = Job.getInstance(conf, this.getClass().getName());

		job.setJobName(this.getClass().getName() + ".jar");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setJarByClass(FilterMostRelevantAnchorRevision.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job,
				org.apache.hadoop.io.compress.BZip2Codec.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static String entityFile = "/home/khoi/follow_up_websci_task/data/200_entities_list_and_letor_id";
	public static String[] entityList = null;
	public static String[] getSampleEntities(String file_name) {

		try {
			File f = new File(file_name);
			BufferedReader br = new BufferedReader(new FileReader(f));
			String rl = null;
			ArrayList<String> arr = new ArrayList<String>();
			while ((rl = br.readLine()) != null) {
				arr.add(rl.replace('_', ' ').toLowerCase());
			}
			String[] strArr = new String[arr.size()];
			br.close();
			return arr.toArray(strArr);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new FilterMostRelevantAnchorRevision(), args);
		System.exit(res);
	}
}