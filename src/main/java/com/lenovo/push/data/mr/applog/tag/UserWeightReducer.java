package com.lenovo.push.data.mr.applog.tag;


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.lenovo.push.data.mr.applog.io.TextPair;


/**
 * input:
 * key: <app,weight> or <app, "">
 * value: user,type(download,install,use,uninstall) or ""
 * output:
 * key: user
 * value: weight
 * 
 * @author gulei2
 *
 */
public class UserWeightReducer extends Reducer<TextPair, Text, Text, Text> {

	@Override
	public void reduce(TextPair key, Iterable<Text> valueSet, Context context) throws IOException, InterruptedException {
		//String app = key.getFirst().toString();
		String weight = key.getSecond().toString();
		if (weight.equals("")) return;
		else {
			for (Text value : valueSet) {
				// value: user,type(download,install,use,uninstall) or ""
				String[] seg = value.toString().split(",");
				if (seg.length == 2) {
					if (seg[1].equals("uninstall")) {
						context.write(new Text(seg[0]), new Text(negative(weight)));
					} else {
						context.write(new Text(seg[0]), new Text(weight));
					}
				}
			}
		}		
	}
	
	private static String negative(String s) {
		String[] ss = s.split(",");
		StringBuffer sb = new StringBuffer();
		sb.append("-" + ss[0]);
		for (int i = 1; i < ss.length; i++) {
			sb.append("," + "-" + ss[i]);
		}
		return sb.toString();
	}

	public static void main(String[] args) {
		System.out.println(negative("1.1,4.4,2,3.0"));
	}
}
