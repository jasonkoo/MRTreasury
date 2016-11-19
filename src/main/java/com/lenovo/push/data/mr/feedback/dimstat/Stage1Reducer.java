package com.lenovo.push.data.mr.feedback.dimstat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 *  Input: dimname<adid\001dimval\001thedate:eventname>pid  -> [1,...1]
 *  Output: dimname<adid\001dimval\001thedate:eventname>pid -> NULL
 * 
 * @author gulei2
 *
 */

public class Stage1Reducer extends Reducer<Text, IntWritable, Text, NullWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> vals, Context context) throws IOException, InterruptedException {
		context.write(key, NullWritable.get());
	}
}
