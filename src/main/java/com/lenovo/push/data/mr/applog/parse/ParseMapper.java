package com.lenovo.push.data.mr.applog.parse;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Input:
 * 2015-08-06 00:00:00	|864566024537834|Lenovo A360t|4.4.2_LSFLSF|com.lenovo.lsf.device.phone|405021446|V4.5.2.1446pi|FF5F8278CADEEBD2E78BC9FEAEED4654
 * Output:
 * key: deviceId,uploadTime
 * value: pkgName
 * 
 * @author gulei2
 *
 */
public class ParseMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private String uploadTime;
	private String deviceId;
	private String pkgName;
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] part = value.toString().trim().split("\t");
		if (part.length == 2) {
			uploadTime = part[0];
			String[] parts = part[1].trim().split("\\|");
			deviceId = parts[1];
			pkgName = parts[4];
			context.write(new Text(deviceId + "," + uploadTime), new Text(pkgName));
		} 
	}	
}
