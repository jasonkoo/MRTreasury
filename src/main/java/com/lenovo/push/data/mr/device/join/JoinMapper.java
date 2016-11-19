package com.lenovo.push.data.mr.device.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Input:
 * deviceid\001devicemodel\001countrycode\001cityname\001peversion\001pevercode\001updatetime
 * deviceid\001tag1,tag2,tag3...
 * Output:
 * key: deviceid
 * value: devicemodel\001countrycode\001cityname\001peversion\001pevercode\001updatetime
 *        tag1,tag2,tag3...
 * @author gulei2
 *
 */
public class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static String FIELD_SEP = "\001";
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		if (line.indexOf(FIELD_SEP) > 0) {
			String deviceid = line.substring(0, line.indexOf(FIELD_SEP));
			String tail = line.substring(line.indexOf(FIELD_SEP) + 1);
			context.write(new Text(deviceid), new Text(tail));
		}
	}
}
