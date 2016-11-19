package com.lenovo.push.data.mr.feedback.dimstat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * Input: dimname<adid\001dimval\001thedate:eventname -> [1,...1]
 * Output: dimname<adid\001dimval\001thedate:eventname -> sum
 * 
 * @author gulei2
 *
 */

public class Stage2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> vals, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : vals) {
			sum += val.get();
		}
		context.write(key, new IntWritable(sum));
	}
}
