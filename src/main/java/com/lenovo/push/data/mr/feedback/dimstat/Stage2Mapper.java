package com.lenovo.push.data.mr.feedback.dimstat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Input: dimname<adid\001dimval\001thedate:eventname>pid 
 * Output: dimname<adid\001dimval\001thedate:eventname -> 1
 * 
 * @author gulei2
 *
 */

public class Stage2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private IntWritable one = new IntWritable(1);
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] parts = line.split(">");
		if (parts.length == 2) {
			context.write(new Text(parts[0]), one);
		}		
	}
}
