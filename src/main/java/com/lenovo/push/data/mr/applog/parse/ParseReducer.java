package com.lenovo.push.data.mr.applog.parse;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Input:
 * key: deviceId,uploadTime
 * value: pkgName
 * Output:
 * key: nullWritable
 * value: deviceId\001pkgName1,pkgName2,.......\001install\001uploadTime
 * 
 * @author gulei2
 *
 */
public class ParseReducer extends Reducer<Text, Text, NullWritable, Text> {
	
	private String deviceId;
	private String uploadTime;
	
	@Override
	public void reduce(Text key, Iterable<Text> valueSet, Context context) throws IOException, InterruptedException {
		String[] parts =  key.toString().split(",");
		if (parts.length == 2) {
			deviceId = parts[0];
			uploadTime = parts[1];
			StringBuilder sb = new StringBuilder();
			sb.append(deviceId);
			sb.append("\001");
			for (Text val : valueSet) {
				sb.append(val.toString());
				sb.append(",");
			}
			sb.deleteCharAt(sb.length() - 1);
			sb.append("\001");
			sb.append("install");
			sb.append("\001");
			sb.append(uploadTime);
			
			context.write(NullWritable.get(), new Text(sb.toString()));
		}
	}
}
