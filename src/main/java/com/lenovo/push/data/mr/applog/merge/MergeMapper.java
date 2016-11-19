package com.lenovo.push.data.mr.applog.merge;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Input:
 * deviceId\001pkgName1,pkgName2,.......\001install\001uploadTime
 * Output:
 * key: deviceId
 * value: pkgName1,pkgName2,.......\001install\001uploadTime
 * 
 * @author gulei2
 *
 */
public class MergeMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] part = value.toString().split("\001");
		if (part.length == 4) {
			StringBuilder sb = new StringBuilder();
			sb.append(part[1]);
			sb.append("\001");
			sb.append(part[2]);
			sb.append("\001");
			sb.append(part[3]);
			context.write(new Text(part[0]), new Text(sb.toString()));
		}
	}
}
