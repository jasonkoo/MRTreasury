package com.lenovo.push.data.mr.feedback.dimstat;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Input: dbtbl\001logtime\001pid\001bizType\001eventName\001feedbackId\001success\001sid\001errCode\001packName\001currVer\001targetVer\001value\001stepId
 * \001deviceid\001devicemodel\001countrycode\001cityname\001peversion\001pevercode\001operationtype\001deviceidtype\001channelname\001custversion\001imsi\001osversion\001pepkg\001pollversion
 * Output: dimname<adid\001dimval\001thedate:eventname>pid -> 1
 * 
 * @author gulei2
 *
 */


public class Stage1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static String FIELD_SEP = "\001";
	private String thedate;
	private HashSet<String> successEventNameSet = new HashSet<String>(){
		private static final long serialVersionUID = 1L;
		{
			add("push");
			add("arrive");
			add("display");
			add("click");
			add("download");
			add("install");
			add("activate");
		}
	};
	private HashSet<String> failureEventNameSet = new HashSet<String>(){
		private static final long serialVersionUID = 1L;
		{
			add("download");
			add("install");
		}
	};	
	
	public void setup(Context context) throws IOException ,InterruptedException {
		Configuration conf = context.getConfiguration();
		this.thedate = conf.get("thedate");		
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] parts = value.toString().split(FIELD_SEP);
		if (parts.length == 28 && parts[7].startsWith("rsys") && parts[5].startsWith("rinter2")) { // only compute for pushmarketing and fake
		
			if (parts[6].equals("true") && successEventNameSet.contains(parts[4])) { // success count
				HashMap<String, String> nameValuePair = new HashMap<String, String>();
				String devicemodel = parts[15];
				String cityname = parts[17];
				String peversion = parts[18];
				if (devicemodel != null && devicemodel.length() > 0 && devicemodel.length() <= 30 && (devicemodel.startsWith("Lenovo") || devicemodel.startsWith("IdeaTab") || devicemodel.startsWith("ZUK"))) {
					// satisfy the condition and do nothing at all
				} else {
					devicemodel = "NULL";
				}
			
				if(cityname != null && cityname.length() > 0 && cityname.length() <= 30 && !cityname.equals("null") && !cityname.equals("unknown")) {
					// satisfy the condition and do nothing at all
				} else {
					cityname = "NULL";
				}
			
				if( peversion != null && peversion.length() > 0 && peversion.length() <= 30 && !peversion.equals("null") && !peversion.equals("unknown")) {
					// satisfy the condition and do nothing at all
				} else {
					peversion = "NULL";
				}
				
				nameValuePair.put("devicemodel", devicemodel);
				nameValuePair.put("cityname", cityname);
				nameValuePair.put("peversion", peversion);
				
				for (String name : nameValuePair.keySet()) {
					StringBuffer sb = new StringBuffer();
					sb.append(name); // dimname
					sb.append("<");
					sb.append(parts[5]); // adid
					sb.append("\001");
					sb.append(nameValuePair.get(name)); // dimval
					sb.append("\001");
					sb.append(thedate); // thedate
					sb.append(":");
					sb.append(parts[4]); // eventname
					sb.append(">");
					sb.append(parts[2]); // pid
					context.write(new Text(sb.toString()), new IntWritable(1));
				}
				
				
				
			} else if (parts[6].equals("false") && failureEventNameSet.contains(parts[4])) { // failure count
				HashMap<String, String> nameValuePair = new HashMap<String, String>();
				String devicemodel = parts[15];
				String cityname = parts[17];
				String peversion = parts[18];
				
				if (devicemodel != null && devicemodel.length() > 0 && devicemodel.length() <= 30 && (devicemodel.startsWith("Lenovo") || devicemodel.startsWith("IdeaTab"))) {
					// satisfy the condition and do nothing at all
				} else {
					devicemodel = "NULL";
				}
				if (cityname != null && cityname.length() > 0 && cityname.length() <= 30 && !cityname.equals("null") && !cityname.equals("unknown")) {
					// satisfy the condition and do nothing at all
				} else {
					cityname = "NULL";
				}
				
				if (peversion != null && peversion.length() > 0 && peversion.length() <= 30 && !peversion.equals("null") && !peversion.equals("unknown")) {
					// satisfy the condition and do nothing at all
				} else {
					peversion = "NULL";
				}
				
				nameValuePair.put("devicemodel", devicemodel);
				nameValuePair.put("cityname", cityname);
				nameValuePair.put("peversion", peversion);
				
				for (String name : nameValuePair.keySet()) {
					StringBuffer sb = new StringBuffer();
					sb.append(name); // dimname
					sb.append("<");
					sb.append(parts[5]); // adid
					sb.append("\001");
					sb.append(nameValuePair.get(name)); // dimval
					sb.append("\001");
					sb.append(thedate); // thedate
					sb.append(":");
					sb.append(parts[4]); // eventname
					sb.append("fail");   // add the "fail" flag for failure 
					sb.append(">");
					sb.append(parts[2]); // pid
					context.write(new Text(sb.toString()), new IntWritable(1));
				}
								
			}
		}		
	}	
}
