package com.lenovo.push.data.mr.applog.merge;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Input:
 * key: deviceId
 * value: pkgName1,pkgName2,.......\001install\001uploadTime
 * Output:
 * key: nullWritable
 * value: deviceId\001pkgName1,pkgName2,.......\001install\001maxUploadTime
 * 
 * @author gulei2
 *
 */
public class MergeReducer extends Reducer<Text, Text, NullWritable, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> valueSet, Context context) throws IOException, InterruptedException {
		String deviceId = key.toString();
		String maxUploadTime = "0000-00-00 00:00:00";
		String resultVal = null;
		
		for (Text val : valueSet) {
			String[] parts = val.toString().split("\001");
			if (parts.length == 3 && parts[2].compareTo(maxUploadTime) > 0) {
				maxUploadTime = parts[2];
				resultVal = val.toString();
			}
		}
		if (resultVal != null) {
			context.write(NullWritable.get(), new Text(deviceId + "\001" + resultVal));
		}		
	}
}
