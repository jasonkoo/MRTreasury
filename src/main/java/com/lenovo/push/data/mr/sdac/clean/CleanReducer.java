package com.lenovo.push.data.mr.sdac.clean;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Input:
 * key: imei,logtime
 * value: [dbtbl,logtime,method,times,version,networkMode,v,imsi,imei,s,l,p,imsi2,c,ip,f,imei2,isRetry,systemID,cellID,latitude,longitude,province,city,district,coTime,themonth,thedate,countrycode,logtime]
 * Output:
 * key: null
 * value: dbtbl,logtime,method,times,version,networkMode,v,imsi,imei,s,l,p,imsi2,c,ip,f,imei2,isRetry,systemID,cellID,latitude,longitude,province,city,district,coTime,themonth,thedate,countrycode,isFirstTime
 *
 */
public class CleanReducer extends Reducer<Text, Text, NullWritable, Text> {
	
	private static String FIELD_SEP = "\001";
	private static String OUTPUT_SEP = "\002";
	private static String PAIR_SEP = "\003";
	
	private static String TRUE = "true";
	private static String FALSE = "false";
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String[] imeiTimePair = key.toString().split(PAIR_SEP);
		
		String minLogTime = imeiTimePair[1];
		
		Boolean alreadyTaged = false;
		for (Text val : values) {
			String line = val.toString();
			String[] parts = line.split(OUTPUT_SEP);
			if (parts.length == 2) {
				if (!alreadyTaged) {
					if ( parts[1].compareTo(minLogTime) == 0) {
						context.write(NullWritable.get(), new Text(parts[0] + FIELD_SEP + TRUE));
						alreadyTaged = true;
					} else if (parts[1].compareTo(minLogTime) > 0) {
						context.write(NullWritable.get(), new Text(parts[0] + FIELD_SEP + FALSE));
					}
				} else {
					context.write(NullWritable.get(), new Text(parts[0] + FIELD_SEP + FALSE));
				}
			}
		}
 	}
}
