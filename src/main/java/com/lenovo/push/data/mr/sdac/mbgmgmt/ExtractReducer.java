package com.lenovo.push.data.mr.sdac.mbgmgmt;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Input:
 * key: imei
 * value: [devicemodel.country.thedate]
 * Output:
 * key: imei
 * value: devicemodel.country.thedate
 *
 */
public class ExtractReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Text value = null;
		for (Text val : values) {
			value = val;
		}
		context.write(key, value);
 	}
}
