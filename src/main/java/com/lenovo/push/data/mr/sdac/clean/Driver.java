package com.lenovo.push.data.mr.sdac.clean;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Cleaning sdac+: 
 * 1) add countrycode column from ip parsing
 * 2) add isFirstTime column
 * 
 * @author gulei2
 *
 */


public class Driver extends Configured implements Tool {
		
	public static final String INPUTPATH = "inputPath";
	public static final String OUTPUTPATH = "outputPath";
	public static final String NUMREDUCERS = "numReducers";
	

	public int run(String[] args) throws Exception {
		
		Configuration conf = this.getConf();
		String inputPath = conf.get(INPUTPATH);
		String outputPath = conf.get(OUTPUTPATH);
		int numReducers = conf.getInt(NUMREDUCERS, 10);
		
		System.out.println("inputPath: " + inputPath);
		System.out.println("outputPath: " + outputPath);
		System.out.println("numReducers: " + numReducers);
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(outputPath))) {
			fs.delete(new Path(outputPath), true);
		}   	
	
		Job job1 = Job.getInstance(conf);
		
		job1.setJobName("sdac+ cleaning " + new Date());
		job1.setJarByClass(Driver.class);
		
		FileInputFormat.setInputPaths(job1, new Path(inputPath));
		FileOutputFormat.setOutputPath(job1, new Path(outputPath));
		
		job1.setMapperClass(CleanMapper.class);
		job1.setPartitionerClass(FirstPartitioner.class);
		job1.setSortComparatorClass(SortComparator.class);
		job1.setGroupingComparatorClass(GroupComparator.class);
		job1.setReducerClass(CleanReducer.class);
		
		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setNumReduceTasks(numReducers);
		
		return job1.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class FirstPartitioner extends Partitioner<Text, Text> {
		
		private static String PAIR_SEP = "\003";
		
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String[] imeiTimePair = key.toString().split(PAIR_SEP);
			String imei = imeiTimePair[0];			
			return Math.abs(imei.hashCode()) % numPartitions;
		}
		
	}
	
	public static class SortComparator extends WritableComparator {
		protected SortComparator() {
			super(Text.class, true);
		}
		
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			String imeiTimePair1 = ( (Text) w1 ).toString();
			String imeiTimePair2 = ( (Text) w2 ).toString();
 			return imeiTimePair1.compareTo(imeiTimePair2);
		}
	}
	
	public static class GroupComparator extends WritableComparator {
		
		private static String PAIR_SEP = "\003";
		
		protected GroupComparator() {
			super(Text.class, true);
		}
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			String imei1 = ( (Text) w1 ).toString().split(PAIR_SEP)[0];
			String imei2 = ( (Text) w2 ).toString().split(PAIR_SEP)[0];
			return imei1.compareTo(imei2);
		}
	}
	
	public static void main(String[] args) throws Exception {		
		int res = ToolRunner.run(new Configuration(), new Driver(), args);
		System.exit(res);
	}
}
