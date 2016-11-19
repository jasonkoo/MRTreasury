package com.lenovo.push.data.mr.feedback.dimstat;

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
 * Join feedback data with device attribute info from MongoDB
 * and 
 * compute daily distributions of each event and failure in dimensions of devicemodel, cityname and peversion
 * 
 * @author gulei2
 *
 */


public class Driver extends Configured implements Tool {	
	
	private static final String INPUTPATH = "inputPath";
	private static final String STAGE1OUTPUTPATH = "stage1OutputPath";
	private static final String STAGE2OUTPUTPATH = "stage2OutputPath";
	private static final String STAGE3BASEOUTPUTPATH = "stage3BaseOutputPath";
	private static final String NUMREDUCERS = "numReducers";
	
	private static final String THEDATE = "thedate";
  
	public int run(String[] args) throws Exception {
		
		Configuration conf = this.getConf();		
		
		String inputPath = conf.get(INPUTPATH);
		String stage1OutputPath = conf.get(STAGE1OUTPUTPATH);
		String stage2OutputPath = conf.get(STAGE2OUTPUTPATH);
		String stage3BaseOutputPath = conf.get(STAGE3BASEOUTPUTPATH);
		int numReducers = conf.getInt(NUMREDUCERS, 10);
		
		int numReducersStage1 = numReducers;
		int numReducersStage2 = numReducers;
		int numReducersStage3 = numReducers;
				
		//int numReducersStage2 = (int) 0.2 * numReducers;
		//int numReducersStage3 = (int) 0.1 * numReducers;
		
		String thedate = conf.get(THEDATE);
		
		System.out.println("inputPath: " + inputPath);
		System.out.println("stage1OutputPath: " + stage1OutputPath);
		System.out.println("stage2OutputPath: " + stage2OutputPath);
		System.out.println("stage3BaseOutputPath: " + stage3BaseOutputPath);
		System.out.println("numReducers: " + numReducers);
		System.out.println("thedate: " + thedate);
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(stage1OutputPath))) {
			fs.delete(new Path(stage1OutputPath), true);
		}   	
	
		if (fs.exists(new Path(stage2OutputPath))) {
			fs.delete(new Path(stage2OutputPath), true);
		}   	
	
		if (fs.exists(new Path(stage3BaseOutputPath))) {
			fs.delete(new Path(stage3BaseOutputPath), true);
		}   	
	
	
		Job job1 = Job.getInstance(conf);
		
		job1.setJobName("Stage1 " + new Date() + ": " + inputPath);
		job1.setJarByClass(Driver.class);
		
		FileInputFormat.setInputPaths(job1, new Path(inputPath));
		FileOutputFormat.setOutputPath(job1, new Path(stage1OutputPath));
		
		job1.setMapperClass(Stage1Mapper.class);
		job1.setReducerClass(Stage1Reducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(NullWritable.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		
		job1.setNumReduceTasks(numReducersStage1);
		
		System.out.println(job1.getJobName() + " starting!" );
		if (job1.waitForCompletion(true)) {
			System.out.println(job1.getJobName() + " done!");
			
			Job job2 = Job.getInstance(conf);
			
			job2.setJobName("Stage2 " + new Date() + ": " + stage1OutputPath);
			job2.setJarByClass(Driver.class);
			
			FileInputFormat.setInputPaths(job2, new Path(stage1OutputPath));
			FileOutputFormat.setOutputPath(job2, new Path(stage2OutputPath));
			
			job2.setMapperClass(Stage2Mapper.class);
			job2.setReducerClass(Stage2Reducer.class);
			
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(IntWritable.class);
			
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(IntWritable.class);
			
			job2.setNumReduceTasks(numReducersStage2);
			
			System.out.println(job2.getJobName() + " starting!" );
			if (job2.waitForCompletion(true)) {
				System.out.println(job2.getJobName() + " done!");
				
				Job job3 = Job.getInstance(conf);
				
				job3.setJobName("Stage3 " + new Date() + ": " + stage2OutputPath);
				job3.setJarByClass(Driver.class);
				
				FileInputFormat.setInputPaths(job3, new Path(stage2OutputPath));
				FileOutputFormat.setOutputPath(job3, new Path(stage3BaseOutputPath));
				
				job3.setMapperClass(Stage3Mapper.class);
				job3.setReducerClass(Stage3Reducer.class);
				
				job3.setOutputKeyClass(Text.class);
				job3.setOutputValueClass(NullWritable.class);
				
				job3.setMapOutputKeyClass(Text.class);
				job3.setMapOutputValueClass(Text.class);				
			
				job3.setNumReduceTasks(numReducersStage3);
				
				System.out.println(job3.getJobName() + " starting!" );
				if (job3.waitForCompletion(true)) {
					System.out.println(job3.getJobName() + " done!");
					return 0;
				} else {
					System.out.println(job3.getJobName() + " fails!");
					return 3;
				}
				
			} else {
				System.out.println(job2.getJobName() + " fails!");
				return 2;
			}
			
		} else {
			System.out.println(job1.getJobName() + " fails!");
			return 1;
		}
	}	
	
	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new Driver(), args);
		System.exit(res);
	}
}
