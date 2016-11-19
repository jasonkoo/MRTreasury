package com.lenovo.push.data.mr.applog.tag;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * input<br>
 * user\tapp对标签1的权重,app对标签2的权重,...<br><br>
 * 
 * @author hanxiaoten
 *
 */
public class UserWeightMapper2 extends Mapper<Object, Text, Text, Text> {

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] part = value.toString().trim().split("\\t");
		if (part.length == 2) {
			context.write(new Text(part[0]), new Text(part[1] + ",1"));
		} else {
			return;
		}
	}
	
}