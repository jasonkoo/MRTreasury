package com.lenovo.push.data.mr.applog.tag;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * input<br>
 * user,user对标签1的权重,user对标签2的权重,...,总计数<br><br>
 * 
 * @author hanxiaoten
 *
 */
public class UserWeightMapper3 extends Mapper<Object, Text, Text, Text> {

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String s = value.toString();
		int first = s.indexOf(",");
		int last = s.lastIndexOf(",");
		context.write(new Text(s.substring(0, first)), new Text(s.substring(first + 1, last)));
	}
	
}