package com.lenovo.push.data.mr.device.merge;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Input:
 * key: pid
 * value: updatetime,pid\001deviceid\001devicemodel\001countrycode\001cityname\001peversion\001pevercode\001operationtype\001deviceidtype\001channelname\001custversion\001imsi\001osversion\001pepkg\001pollversion\001updatetime
 * Output:
 * key: NullWritable
 * value: pid\001deviceid\001devicemodel\001countrycode\001cityname\001peversion\001pevercode\001operationtype\001deviceidtype\001channelname\001custversion\001imsi\001osversion\001pepkg\001pollversion\001updatetime
 * 
 * @author gulei2
 *
 */
public class MergeReducer extends Reducer<Text, Text, NullWritable, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> valueSet, Context context) throws IOException, InterruptedException {
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
			context.write(NullWritable.get(), new Text(latestLine));
		}
		
	}
}
