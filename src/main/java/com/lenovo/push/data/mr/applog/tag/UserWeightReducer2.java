package com.lenovo.push.data.mr.applog.tag;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserWeightReducer2 extends Reducer<Text, Text, Text, Text> {
	public static int varNum = 33;

	@Override
	public void reduce(Text key, Iterable<Text> valueSet, Context context) throws IOException, InterruptedException {
		float[] sum = new float[varNum];
		Arrays.fill(sum, 0.0f);
		int count = 0;
		for (Text value : valueSet) {
			String[] seg = value.toString().split(",");
			if (seg.length == varNum + 1) {
				for (int i = 0; i < varNum; i++) {
					sum[i] += Float.parseFloat(seg[i]);
				}
				count += Integer.parseInt(seg[varNum]);
			} else {
				context.write(null, new Text("wrong format!"));
			}
		}

		// scale up sum
		StringBuilder result = new StringBuilder();
		result.append(key.toString() + ",");
		result.append(sum[0] / (float) count);
		for (int i = 1; i < varNum; i++) {
			sum[i] = sum[i] / (float) count;
			if (Float.isNaN(sum[i])) {
				return;
			}
			/*if (sum[i] == Float.NaN) {
				return;
			}*/
			result.append(",");
			result.append(sum[i]);
		}
		result.append("," + count);
		context.write(null, new Text(result.toString()));
	}	
}
