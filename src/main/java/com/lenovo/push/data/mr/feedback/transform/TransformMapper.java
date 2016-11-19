package com.lenovo.push.data.mr.feedback.transform;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Input:
 * dbtbl\001logtime\001pid\001bizType\001eventName\001feedbackId\001success\001sid\001errCode\001packName\001currVer\001targetVer\001value
 * \001deviceid\001devicemodel\001countrycode\001cityname\001peversion\001pevercode\001operationtype\001deviceidtype\001channelname\001custversion\001imsi\001osversion\001pepkg\001pollversion
 * Output:
 * dbtbl\001logtime\001pid\001bizType\001eventName\001feedbackId\001success\001sid\001errCode\001packName\001currVer\001targetVer\001value\stepId
 * \001deviceid\001devicemodel\001countrycode\001cityname\001peversion\001pevercode\001operationtype\001deviceidtype\001channelname\001custversion\001imsi\001osversion\001pepkg\001pollversion
 * @author gulei2
 *
 */
public class TransformMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private static String FIELD_SEP = "\001";
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		int count = StringUtils.countMatches(line, FIELD_SEP);
		if (count == 26) {
			context.write(NullWritable.get(), new Text(insertStepId(line)));
		} else if (count == 27) {
			context.write(NullWritable.get(), new Text(line));
		}
		
	}
	
	private String insertStepId(String line) {	
		String[] parts = line.split(FIELD_SEP);
		String[] partsStepIdInserted = insertElement(parts, "", 13);
		return concatParts(partsStepIdInserted);
	}
	
	private static String[] insertElement(String original[], String element, int index) {
		int length = original.length;
	  	String destination[] = new String[length + 1];
	  	System.arraycopy(original, 0, destination, 0, index);
	  	destination[index] = element;
	  	System.arraycopy(original, index, destination, index + 1, length - index);
	  	return destination;
	}
	
	private static String concatParts(String[] parts) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < parts.length; i++) {
			sb.append(parts[i]);
			sb.append(FIELD_SEP);
		}
		sb.deleteCharAt(sb.length() - 1);
		return sb.toString();
	}
	
	public static void main(String[] args) {
		String line = "domestic.feedback20150830 14:32:00324085892displayrinter2_2c91bc544f1b600a014f4e76df660098falsersysERROR_EXPIRED_TIME860882027785948Lenovo A398tCN_湖南益阳V4.5.2.1446pi405021446MOBimeicom.lenovo.lsf.deviceLenovo A398t_S227_1406044.0.3com.lenovo.lsf.device03";
		TransformMapper tm = new TransformMapper();
		System.out.println(line);
		System.out.println(tm.insertStepId(line));
	}
}
