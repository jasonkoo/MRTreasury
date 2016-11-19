package com.lenovo.push.data.mr.sdac.mbgmgmt;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Stat sdac devicemodel,country distribution for MBGMGMT
 * @author gulei2
 *
 */


public class Driver extends Configured implements Tool {
		
	public static final String INPUTPATHS = "inputPaths";
	public static final String OUTPUTPATH_STAGE1 = "outputPathStage1";
	public static final String OUTPUTPATH_STAGE2 = "outputPathStage2";
	public static final String STARTTIME = "startTime";
	public static final String ENDTIME = "endTime";
	public static final String PREFIX = "sdac.mbgmgmt.";
	public static final String NUMREDUCERS = "numReducers";

	public int run(String[] args) throws Exception {
		
		Configuration conf = this.getConf();
		String inputPaths = conf.get(INPUTPATHS);
		String outputPathStage1 = conf.get(OUTPUTPATH_STAGE1);
		String outputPathStage2 = conf.get(OUTPUTPATH_STAGE2);
		String startTime = conf.get(STARTTIME);
		String endTime = conf.get(ENDTIME);
		int numReducers = conf.getInt(NUMREDUCERS, 10);
		
		System.out.println("inputPaths: " + inputPaths);
		System.out.println("outputPathStage1: " + outputPathStage1);
		System.out.println("outputPathStage2: " + outputPathStage2);
		System.out.println("startTime: " + startTime);
		System.out.println("endTime: " + endTime);
		System.out.println("numReducers: " + numReducers);
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(outputPathStage1))) {
			fs.delete(new Path(outputPathStage1), true);
		}   	
		if (fs.exists(new Path(outputPathStage2))) {
			fs.delete(new Path(outputPathStage2), true);
		}   	
	
	
		Job job1 = Job.getInstance(conf);
		
		job1.setJobName("MBGMGMT SDAC+ Extraction " + new Date());
		job1.setJarByClass(Driver.class);
		
		FileInputFormat.setInputPaths(job1, inputPaths);
		FileOutputFormat.setOutputPath(job1, new Path(outputPathStage1));
		
		job1.setMapperClass(ExtractMapper.class);
		job1.setReducerClass(ExtractReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setNumReduceTasks(10);
		
		if (job1.waitForCompletion(true)) {
			Job job2 = Job.getInstance(conf);
			
			job2.setJobName("MBGMGMT SDAC+ Stat " + new Date());
			job2.setJarByClass(Driver.class);
			
			FileInputFormat.setInputPaths(job2, new Path(outputPathStage1));
			FileOutputFormat.setOutputPath(job2, new Path(outputPathStage2));
			
			job2.setMapperClass(StatMapper.class);
			job2.setReducerClass(StatReducer.class);
			
			job2.setOutputKeyClass(NullWritable.class);
			job2.setOutputValueClass(Text.class);
			
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(IntWritable.class);
			
			job2.setNumReduceTasks(5);
			
			return job2.waitForCompletion(true) ? 0 : 1;
		} else {
			return 1;
		}
	}	
	
	public static void main(String[] args) throws Exception {		
		int res = ToolRunner.run(new Configuration(), new Driver(), args);
		System.exit(res);
	}
}
