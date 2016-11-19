package com.lenovo.push.data.mr.sdac.mbgmgmt;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Input:
 * key: imei
 * value: devicemodel.country.thedate
 * Output:
 * key: sdac.mbgmgmt.devicemodel.country.thedate
 * value: 1
 *
 */
public class StatMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private IntWritable one = new IntWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] parts = line.split("\t");
		String theKey = Driver.PREFIX + parts[1];
		context.write(new Text(theKey), one);
	}
}
