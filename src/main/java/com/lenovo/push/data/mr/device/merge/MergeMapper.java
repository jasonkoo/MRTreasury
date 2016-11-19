package com.lenovo.push.data.mr.device.merge;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Input:
 * pid\001deviceid\001devicemodel\001countrycode\001cityname\001peversion\001pevercode\001operationtype\001deviceidtype\001channelname\001custversion\001imsi\001osversion\001pepkg\001pollversion\001updatetime
 * Output:
 * key: pid
 * value: updatetime,pid\001deviceid\001devicemodel\001countrycode\001cityname\001peversion\001pevercode\001operationtype\001deviceidtype\001channelname\001custversion\001imsi\001osversion\001pepkg\001pollversion\001updatetime
 * @author gulei2
 *
 */
public class MergeMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static String FIELD_SEP = "\001";
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] parts = line.split(FIELD_SEP);
		if (parts.length == 16) {
			context.write(new Text(parts[0]), new Text(parts[15] + "," + line));
		}
	}
}
