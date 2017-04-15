package de.l3s.hadoop.mapreduce.demo;

import java.io.*;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

class DBOutputWritable implements Writable, DBWritable {

	private long doc_id;
	private int inlink;
	private double domain_pagerank;
	private double core_pagerank;
	private int URLDepth;
	private int Revision;
	private int domain_size; 
	private int AnchorTimeSpans;

	public DBOutputWritable(long doc_id, int inlink, double domain_pagerank, double core_pagerank, int URLDepth, int Revision, int domain_size, int AnchorTimeSpans) {
		this.doc_id = doc_id;
		this.inlink = inlink;
		this.domain_pagerank = domain_pagerank;
		this.core_pagerank = core_pagerank;
		this.URLDepth = URLDepth;
		this.Revision = Revision;
		this.domain_size = domain_size;
		this.AnchorTimeSpans = AnchorTimeSpans;
	}

	public void readFields(DataInput in) throws IOException {
	}

	public void readFields(ResultSet rs) throws SQLException {
		doc_id = rs.getInt(1);
		inlink = rs.getInt(2);
		domain_pagerank = rs.getDouble(3);
		core_pagerank = rs.getDouble(4);
		URLDepth = rs.getInt(5);
		Revision = rs.getInt(6);
		domain_size = rs.getInt(7);
		AnchorTimeSpans = rs.getInt(8);
	}

	public void write(DataOutput out) throws IOException {
	}

	public void write(PreparedStatement ps) throws SQLException {
		ps.setLong(1, doc_id);
		ps.setInt(2, inlink);
		ps.setDouble(3, domain_pagerank);
		ps.setDouble(4, core_pagerank);
		ps.setInt(5, URLDepth);
		ps.setInt(6, Revision);
		ps.setInt(7, domain_size);
		ps.setInt(8, AnchorTimeSpans);
	}

}

public class TransferAnchorArchive2DB extends Configured implements Tool {
	public static final int MAX_LENGTH = 10000;

	private static class Map extends
			Mapper<LongWritable, Text, Text, NullWritable> {

		@Override
		public void map(LongWritable key, Text value, Context output)
				throws IOException, InterruptedException {
			try {
				output.write(value, NullWritable.get());
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

	private static class Reduce extends
			Reducer<Text, NullWritable, DBOutputWritable, NullWritable> {

		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Context output) throws IOException, InterruptedException {
			try {
				String[] fields = key.toString().split("\t");
				
				for (int i = 0; i < fields.length; i ++) {
					if (fields[i].length() == 0) fields[i] = "0";
				}
				
				DBOutputWritable data = new DBOutputWritable(Long.parseLong(fields[0]), Integer.parseInt(fields[3]),
						Double.parseDouble(fields[4]), Double.parseDouble(fields[5]), Integer.parseInt(fields[6]), 
						Integer.parseInt(fields[7]), Integer.parseInt(fields[8]), Integer.parseInt(fields[9]));
				output.write(data, NullWritable.get());
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

		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", 
				"jdbc:mysql://db.l3s.uni-hannover.de:3306/ttran", // db url
				"ttran", // username
				"DFTKKbGV3bxZZb8C"); // password

		conf.setBoolean("map.output.compress", true);
		conf.set("mapreduce.output.compression.type", "BLOCK");
		conf.setClass("mapreduce.output.fileoutputformat.compress.codec",
				org.apache.hadoop.io.compress.BZip2Codec.class,
				org.apache.hadoop.io.compress.CompressionCodec.class);
		conf.set("mapreduce.task.timeout", "1800000");
		conf.set("mapreduce.map.java.opts", "-Xmx3072m");
		conf.set("mapreduce.child.java.opts", "-Xmx3072m");

		Job job = Job.getInstance(conf, "TransferAnchorArchive2DB");

		job.setJobName(this.getClass().getName() + ".jar");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(DBOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(DBWritable.class);
		job.setOutputValueClass(NullWritable.class);

		job.setJarByClass(TransferAnchorArchive2DB.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(10);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		DBOutputFormat.setOutput(job, "khoi_german_archive_features", // output table name
				new String[] { "doc_id", "inlink", "domain_pagerank", "core_pagerank", "URLDepth", "Revision", "domain_size", "AnchorTimeSpans" } // table columns
				);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new TransferAnchorArchive2DB(), args);
		System.exit(res);
	}
}