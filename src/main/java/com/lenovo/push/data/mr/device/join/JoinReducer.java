package com.lenovo.push.data.mr.device.join;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Input:
 * key: deviceid
 * value: devicemodel\001countrycode\001cityname\001peversion\001pevercode\001updatetime
 *        tag1,tag2,tag3...
 * Output:
 * key: NullWritable
 * value: deviceid\001devicemodel\001countrycode\001cityname\001peversion\001pevercode\001updatetime\001tags
 * 
 * @author gulei2
 *
 */
public class JoinReducer extends Reducer<Text, Text, NullWritable, Text> {
	private static String FIELD_SEP = "\001";
	@Override
	public void reduce(Text key, Iterable<Text> valueSet, Context context) throws IOException, InterruptedException {
		String attributes = null;
		String tags = null;
		for (Text val : valueSet) {
			String valString = val.toString();
			if (valString.contains(FIELD_SEP)) {
				attributes = valString;
			} else {
				tags = valString;
			}
		}
		
		if (attributes != null) {
			StringBuilder sb = new StringBuilder();
			sb.append(key.toString());
			sb.append(FIELD_SEP);
			sb.append(attributes);
			sb.append(FIELD_SEP);
			sb.append(tags);
			context.write(NullWritable.get(), new Text(sb.toString()));
		}
	}
}
