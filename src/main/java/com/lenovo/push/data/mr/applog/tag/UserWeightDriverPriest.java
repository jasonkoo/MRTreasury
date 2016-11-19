package com.lenovo.push.data.mr.applog.tag;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.lenovo.push.data.mr.applog.io.FirstPartitioner;
import com.lenovo.push.data.mr.applog.io.KeyComparator;
import com.lenovo.push.data.mr.applog.io.GroupComparator;
import com.lenovo.push.data.mr.applog.io.TextPair;

public class UserWeightDriverPriest {
	
	private static final String INPUT = "input";
	private static final String APPMODEL = "model";
	private static final String TEMP = "temp";
	private static final String OUTPUT = "output";
	private static final String OUTPUT2 = "output2";
	private static final String NUMREDUCERS = "numReducers";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		new GenericOptionsParser(conf, args);
		String input=conf.get(INPUT);
        String model=conf.get(APPMODEL);
        String temp=conf.get(TEMP);
        String output=conf.get(OUTPUT);
        String output2=conf.get(OUTPUT2);
        int numReducers = conf.getInt(NUMREDUCERS, 20);
        
        System.out.println("--------------------------------------------------");
        System.out.println("Arguments passed in: ");
        System.out.println("input: " + input);
        System.out.println("model: " + model);
        System.out.println("temp: " + temp);
        System.out.println("output: " + output);
        System.out.println("output2: " + output2);
        System.out.println("numReducers: " + numReducers);
        System.out.println("--------------------------------------------------");
        
        
        FileSystem fs = FileSystem.get(conf);
    	if (fs.exists(new Path(temp))) {
			fs.delete(new Path(temp), true);
		}
    	
		if (fs.exists(new Path(output))) {
			fs.delete(new Path(output), true);
		}		
	
		if (fs.exists(new Path(output2))) {
			fs.delete(new Path(output2), true);
		}
        
		Job job1 = Job.getInstance(conf);
		job1.setJobName("User Weight at " + new Date() +  ": " + input + "," + model);
		job1.setJarByClass(UserWeightDriverPriest.class);
		
		job1.setMapperClass(UserWeightMapper.class);
		job1.setPartitionerClass(FirstPartitioner.class);
		job1.setSortComparatorClass(KeyComparator.class);
		job1.setGroupingComparatorClass(GroupComparator.class);
		job1.setReducerClass(UserWeightReducer.class);

		job1.setMapOutputKeyClass(TextPair.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setNumReduceTasks(numReducers);
		
		FileInputFormat.setInputPaths(job1, input + "," + model);
		FileOutputFormat.setOutputPath(job1, new Path(temp));
		
		int res1 = job1.waitForCompletion(true) ? 0 : 1;
		if (res1 == 0) {
			System.out.println("job1 done--->");
			Job job2 = Job.getInstance(conf);
			job2.setJobName("User-Tag");
			job2.setJarByClass(UserWeightDriverPriest.class);
			job2.setMapperClass(UserWeightMapper2.class);
			job2.setCombinerClass(UserWeightCombiner2.class);
			job2.setReducerClass(UserWeightReducer2.class);

			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.setNumReduceTasks(numReducers);
			
			Path out = new Path(output);
			FileInputFormat.setInputPaths(job2, temp);
			FileOutputFormat.setOutputPath(job2, out);
			int res2  = job2.waitForCompletion(true) ? 0 : 1;
			if (res2 == 0) {
				System.out.println("job2 done--->");
				Job job3 = Job.getInstance(conf);
				job3.setJobName("User-Bingo");
				job3.setJarByClass(UserWeightDriverPriest.class);
				job3.setMapperClass(UserWeightMapper3.class);
				job3.setReducerClass(UserWeightReducer3.class);
				
				job3.setMapOutputKeyClass(Text.class);
				job3.setMapOutputValueClass(Text.class);
				job3.setOutputKeyClass(Text.class);
				job3.setOutputValueClass(Text.class);
				job3.setNumReduceTasks(numReducers);
				
				Path out2 = new Path(output2);
				FileInputFormat.setInputPaths(job3, out);
				FileOutputFormat.setOutputPath(job3, out2);
				int res  = job3.waitForCompletion(true) ? 0 : 1;
				if (res == 0) {
					System.out.println("job3 done--->");
//					fs.delete(new Path(temp), true); // 删除第一个Reduce的输出目录
				} else {
					System.out.println("job3 failed.");
					return;
				}
			} else {
				System.out.println("job2 failed.");
				return;
			}
		} else {
			System.out.println("job1 failed.");
			return;
		}
	}

}
