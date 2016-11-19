package com.lenovo.push.data.mr.sdac.mbgmgmt;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Input:
 * key: sdac.mbgmgmt.devicemodel.country.thedate
 * value: [1]
 * Output:
 * key: null
 * value: sdac.mbgmgmt.devicemodel.country\001thedate\001count\001lmt
 *
 */
public class StatReducer extends Reducer<Text, IntWritable, NullWritable, Text> {
	
	private static String FIELD_SEP = "\001";
	private SimpleDateFormat formater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private String insertTime;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Date now = new Date();
		this.insertTime = formater.format(now);
	}
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int count = 0;
		for (IntWritable val : values) {
			count += val.get();
		}
		
		
		String keyString = key.toString();		
		String theKey = keyString.substring(0, keyString.lastIndexOf("."));
		String thedate = keyString.substring(keyString.lastIndexOf(".") + 1);
		
		StringBuilder sb = new StringBuilder();
		sb.append(theKey);
		sb.append(FIELD_SEP);
		sb.append(thedate);
		sb.append(FIELD_SEP);
		sb.append(count);
		sb.append(FIELD_SEP);
		sb.append(insertTime);
		
		context.write(NullWritable.get(), new Text(sb.toString()));
	}
	
	public static void main(String[] args) {
		String test = "a.b.c";
		String head = test.substring(0, test.lastIndexOf("."));
		System.out.println(head);
		String tail = test.substring(test.lastIndexOf(".") + 1);
		System.out.println(tail);
	}
}
