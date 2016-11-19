package com.lenovo.push.data.mr.device.importx;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Input:
 * pid<br><br>
 * Output:
 * pid
 * 
 * @author gulei2
 *
 */
public class ImportMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	private static String FIELD_SEP = "\001";
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String pid = value.toString().split(FIELD_SEP)[2];
		context.write(new Text(pid), NullWritable.get());
	}
}
