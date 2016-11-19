package com.lenovo.push.data.mr.device.importx;

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
 * Import device attribute info from MongoDB to HDFS
 * @author gulei2
 *
 */


public class Driver extends Configured implements Tool {	
	
	private static final String ENVTYPE = "envType";
	private static final String PART = "part";
	private static final String INPUTPATH = "inputPath";
	private static final String OUTPUTPATH = "outputPath";
	private static final String NUMREDUCERS = "numReducers";

	public int run(String[] args) throws Exception {
		
		Configuration conf = this.getConf();		
		String envType = conf.get(ENVTYPE);
		String part = conf.get(PART);
		String inputPath = conf.get(INPUTPATH);
		String outputPath = conf.get(OUTPUTPATH);
		int numReducers = conf.getInt(NUMREDUCERS, 40);
		
		System.out.println("envType: " + envType);
		System.out.println("part: " + part);
		System.out.println("inputPath: " + inputPath);
		System.out.println("outputPath: " + outputPath);
		System.out.println("numReducers: " + numReducers);
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(outputPath))) {
			fs.delete(new Path(outputPath), true);
		}   	
	
	
		Job job = Job.getInstance(conf);
		
		job.setJobName("Import " + part + " Device at " + new Date() + ": " + inputPath);
		job.setJarByClass(Driver.class);
		
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setMapperClass(ImportMapper.class);
		job.setReducerClass(ImportReducer.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(numReducers);
		
		return job.waitForCompletion(true) ? 0 : 1;		
	}	
	
	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new Driver(), args);
		System.exit(res);
	}
}
