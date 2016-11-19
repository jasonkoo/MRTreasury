package com.lenovo.push.data.mr.applog.extract;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Extract device tags for solr
 * 
 * @author gulei2
 *
 */

public class Driver extends Configured implements Tool {	
	
	private static final String INPUTPATH = "inputPath";
	private static final String OUTPUTPATH = "outputPath";
	private static final String THRESHOLD = "threshold";

	public int run(String[] args) throws Exception {
		
		Configuration conf = this.getConf();		
		
		String inputPath = conf.get(INPUTPATH);
		String outputPath = conf.get(OUTPUTPATH);
		String threshold = conf.get(THRESHOLD);
		
		System.out.println("inputPath: " + inputPath);
		System.out.println("outputPath: " + outputPath);
		System.out.println("threshold: " + threshold);
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(outputPath))) {
			fs.delete(new Path(outputPath), true);
		}   	
	
	
		Job job = Job.getInstance(conf);
		
		job.setJobName("Extract device tags for Solr at " + new Date() + ": " + inputPath);
		job.setJarByClass(Driver.class);
		
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setMapperClass(ExtractMapper.class);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setNumReduceTasks(0);
		
		return job.waitForCompletion(true) ? 0 : 1;		
	}	
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Driver(), args);
		System.exit(res);
	}
}
