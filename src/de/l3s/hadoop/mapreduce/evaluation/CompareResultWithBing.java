package de.l3s.hadoop.mapreduce.evaluation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CompareResultWithBing extends Configured implements Tool {
	private final static NullWritable empty = NullWritable.get();

	private class DBInputWritable implements Writable, DBWritable {
		private int _rank;
		private String _doc_url;

		public void readFields(DataInput in) throws IOException {
		}

		public void readFields(ResultSet rs) throws SQLException
		{
			_rank = rs.getInt(1);
			_doc_url = rs.getString(2);
		}

		public void write(DataOutput out) throws IOException {
		}

		public void write(PreparedStatement ps) throws SQLException {
			ps.setInt(1, _rank);
			ps.setString(2, _doc_url);
		}

		public int getRank() {
			return _rank;
		}

		public String getUrl() {
			return _doc_url;
		}
		
		public String getRow() {
			return _doc_url + "\t" + _rank;
		}
	}

	public class DBOutputWritable implements Writable, DBWritable {
		private String name;
		private int count;

		public DBOutputWritable(String name, int count) {
			this.name = name;
			this.count = count;
		}

		public void readFields(DataInput in) throws IOException {
		}

		public void readFields(ResultSet rs) throws SQLException {
			name = rs.getString(1);
			count = rs.getInt(2);
		}

		public void write(DataOutput out) throws IOException {
		}

		public void write(PreparedStatement ps) throws SQLException {
			ps.setString(1, name);
			ps.setInt(2, count);
		}
	}

	private static class Map extends
			Mapper<LongWritable, DBInputWritable, Text, NullWritable> {
		private Text record = null;

		@Override
		public void map(LongWritable key, DBInputWritable value, Context output)
				throws IOException, InterruptedException {

			output.write(new Text(value.getRow()), empty);

		}
	}

	private static class Reduce extends
			Reducer<Text, NullWritable, Text, NullWritable> {
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Context output) throws IOException, InterruptedException {

			output.write(key, empty);
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
		Class.forName("com.mysql.jdbc.Driver");
		DBConfiguration
				.configureDB(conf,
						"com.mysql.jdbc.Driver", // driver class
						"jdbc:mysql://prometheus.kbs.uni-hannover.de:3306/archive_bing_big", // db
																								// url
						"archive_bing_r", // user name
						"uxtN99NJ7Wh5J9bN"); // password

		Job job = Job.getInstance(conf, "CompareResultWithBing");

		job.setJobName(this.getClass().getName() + ".jar");
		//job.getClass(com.mysql.jdbc.Driver.class);

		job.setInputFormatClass(DBInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Correct here

//		DBInputFormat.setInput(job, DBInputWritable.class, "pw_result", // input
//				null, null, new String[] { "id", "name" } // table columns
//				);
		
		String query = "SELECT rank, url"   +
				" FROM pw_result r, main_pages_de q" +
				" WHERE q.query_id = r.query_id"     +
				" AND q.title LIKE '%" + args[0]     + "%'" +
				" LIMIT 0 , 100";
		
		DBInputFormat.setInput(job, DBInputWritable.class, query, null);

//		DBOutputFormat.setOutput(job, "output", // output table name
//				new String[] { "rank", "url" } // table columns
//				);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setJarByClass(CompareResultWithBing.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job,
				org.apache.hadoop.io.compress.BZip2Codec.class);

		//FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CompareResultWithBing(), args);
		System.exit(res);
	}
}