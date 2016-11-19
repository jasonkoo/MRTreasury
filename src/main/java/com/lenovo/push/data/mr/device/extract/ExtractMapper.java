package com.lenovo.push.data.mr.device.extract;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.lenovo.push.data.mr.util.CountryCodeUtil;


/**
 * Input:
 * pid\001deviceid\001devicemodel\001countrycode\001cityname\001peversion\001pevercode\001operationtype\001deviceidtype\001channelname\001custversion\001imsi\001osversion\001pepkg\001pollversion\001updatetime
 * Output:
 * key: deviceid
 * value updatetime,devicemodel\001countrycode\001cityname\001peversion\001pevercode
 * @author gulei2
 *
 */
public class ExtractMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private static String FIELD_SEP = "\001";
	private String minUpdateTime;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		this.minUpdateTime = conf.get("minUpdateTime");
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] parts = line.split(FIELD_SEP);
		if (parts.length == 16 && parts[15].compareTo(minUpdateTime) > 0) {
			if (parts[1] != "null" && parts[1] != "") {
				StringBuilder sb = new StringBuilder();
				sb.append(parts[15]);
				sb.append(",");
				sb.append(parts[2]);
				sb.append(FIELD_SEP);
				sb.append(CountryCodeUtil.normalize(parts[3]));
				sb.append(FIELD_SEP);
				sb.append(parts[4]);
				sb.append(FIELD_SEP);
				sb.append(parts[5]);
				sb.append(FIELD_SEP);
				sb.append(parts[6]);
				context.write(new Text(parts[1]), new Text(sb.toString()));
			}
		}
	}
}
