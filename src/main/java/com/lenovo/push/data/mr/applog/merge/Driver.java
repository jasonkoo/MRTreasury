package com.lenovo.push.data.mr.applog.merge;

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


public class Driver extends Configured implements Tool {	
	
	private static final String INPUTPATHS = "inputPaths";
	private static final String OUTPUTPATH = "outputPath";
	private static final String NUMREDUCERS = "numReducers";
		

	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();		
		
		String inputPaths = conf.get(INPUTPATHS);
		String outputPath = conf.get(OUTPUTPATH);
		int numReducers = conf.getInt(NUMREDUCERS, 10);
		
		System.out.println("inputPaths: " + inputPaths);
		System.out.println("outputPath: " + outputPath);
		System.out.println("numReducers: " + numReducers);
	
		
		FileSystem fs = FileSystem.get(conf);
		
		if (fs.exists(new Path(outputPath))) {
			fs.delete(new Path(outputPath), true);
		}
			

		Job job = Job.getInstance(conf);
		
		job.setJobName("Merge applogs at " + new Date() + ": " + inputPaths);
		job.setJarByClass(Driver.class);
		
		FileInputFormat.setInputPaths(job, inputPaths);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setMapperClass(MergeMapper.class);
		job.setReducerClass(MergeReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(numReducers);
		job.waitForCompletion(true);
		
		return 0;
	}	
	
	public static void main(String[] args) throws Exception {		
		ToolRunner.run(new Configuration(), new Driver(), args);
	}
}
