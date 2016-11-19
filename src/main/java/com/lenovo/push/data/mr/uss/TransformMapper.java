package com.lenovo.push.data.mr.uss;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Input:
 * ID,REGISTER_DEVICEID,REGISTER_DATE,REGISTER_DEVICEMODEL,REGISTER_IMSI,REGISTER_SOURCE,REGISTER_DEVICEIMEI,REGISTER_DEVICESN,REGISTER_IP
 * Output:
 * ID,REGISTER_DEVICEID,REGISTER_DATE,REGISTER_DEVICEMODEL,REGISTER_IMSI,REGISTER_SOURCE,REGISTER_DEVICEIMEI,REGISTER_DEVICESN,REGISTER_IP
 *
 */
public class TransformMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private static String FIELD_SEP = ",";
	SimpleDateFormat sourceSDF1 = new SimpleDateFormat("dd-M�� -yy");
	SimpleDateFormat sourceSDF2 = new SimpleDateFormat("dd-M��-yy");
	SimpleDateFormat targetSDF = new SimpleDateFormat("yyyyMMdd");
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] parts = line.split(FIELD_SEP, -1);
		StringBuilder sb = new StringBuilder();
		sb.append(parts[0]);
		for (int i = 1; i < parts.length; i++) {
			sb.append(",");
			if (i == 2) {
				sb.append(convertDate(parts[i]));
			} else {
				sb.append(parts[i]);
			}
		}
		context.write(NullWritable.get(), new Text(sb.toString()));
	}
	
	private String convertDate(String registerDate) {		
		try {
			if(registerDate.contains(" ")) {
				return targetSDF.format(sourceSDF1.parse(registerDate));
			} else {
				return targetSDF.format(sourceSDF2.parse(registerDate));
			}			
		} catch (ParseException e) {
			return null;
		}
	}
	
}