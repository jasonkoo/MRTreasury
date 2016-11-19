package com.lenovo.push.data.mr.device.extract;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Input:
 * key: deviceid
 * value: updatetime,devicemodel\001countrycode\001cityname\001peversion\001pevercode
 * Output:
 * key: NullWritable
 * value: deviceid\001devicemodel\001countrycode\001cityname\001peversion\001pevercode\001updatetime
 * 
 * @author gulei2
 *
 */
public class ExtractReducer extends Reducer<Text, Text, NullWritable, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> valueSet, Context context) throws IOException, InterruptedException {
		String deviceid = key.toString();
		
		String maxUpdateTime = "00000000 00:00:00";
		String latestLine = null;
		for (Text val : valueSet) {
			String[] parts = val.toString().split(",");
			if (parts.length == 2 && parts[0].compareTo(maxUpdateTime) > 0) {
				maxUpdateTime = parts[0];
				latestLine = parts[1];
			}
		}
		if (latestLine != null) {
			StringBuilder sb = new StringBuilder();
			sb.append(deviceid);
			sb.append("\001");
			sb.append(latestLine);
			sb.append("\001");
			sb.append(maxUpdateTime);
			context.write(NullWritable.get(), new Text(sb.toString()));
		}
		
	}
}
