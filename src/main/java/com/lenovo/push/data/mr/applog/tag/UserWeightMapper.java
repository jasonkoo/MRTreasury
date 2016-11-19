package com.lenovo.push.data.mr.applog.tag;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.lenovo.push.data.mr.applog.io.TextPair;

/**
 * input:
 * user\u0001app(app1,app2,...,)\u0001type(download,install,use,uninstall)\u0001uploadtime
 * app,app对标签1的权重,app对标签2的权重,...
 * output:
 * key: <app,weight>
 * value: "" 
 * or 
 * key: <app, "">
 * value: user,type(download,install,use,uninstall)
 * 
 * @author gulei2
 *
 */
public class UserWeightMapper extends Mapper<Object, Text, TextPair, Text> {
	public static int varNum = 33;

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] part = value.toString().trim().split(",");
		if (part.length == (varNum + 1)) {
			String weight = value.toString();
			weight = weight.substring(1 + weight.indexOf(','));
			context.write(new TextPair(part[0], weight), new Text(""));
		} else {
			String[] word = value.toString().trim().split("\u0001");
			if (word.length == 4 && !word[1].equals("null,")) {
				String[] apps = word[1].split(",");
				for (String app : apps) {
					if (!app.equals("")) {
						context.write(new TextPair(app.trim(), ""), new Text(word[0] + "," + word[2]));
					}
				}
			}
		}
	}
}
