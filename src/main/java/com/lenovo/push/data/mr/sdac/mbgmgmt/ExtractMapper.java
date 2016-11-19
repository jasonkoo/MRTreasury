package com.lenovo.push.data.mr.sdac.mbgmgmt;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.lenovo.push.data.ip2location.entity.LocationBean;
import com.lenovo.push.data.ip2location.util.IpConvertUtil;


/**
 * Input:
 * dbtbl,logtime,method,times,version,networkMode,v,imsi,imei,s,l,p,imsi2,c,ip,f,imei2,isRetry,systemID,cellID,latitude,longitude,province,city,district,coTime
 * Output:
 * key: imei
 * value: devicemodel.country.thedate
 *
 */
public class ExtractMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static String FIELD_SEP = "\001";
	private IpConvertUtil util;
	private String startTime;
	private String endTime;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		this.util = IpConvertUtil.getSingleInstance();
		Configuration conf = context.getConfiguration();
		this.startTime = conf.get(Driver.STARTTIME);
		this.endTime = conf.get(Driver.ENDTIME);
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] parts = line.split(FIELD_SEP, -1);
		if (parts.length == 26 && parts[1].compareTo(startTime) >= 0 && parts[1].compareTo(endTime) <= 0 && parts[8].length() > 6 && parts[11].startsWith("Lenovo") && parts[14].trim().length() >= 7){
			String imei = parts[8];
			String devicemodel = null;
			if (parts[11].contains(" ")) {
				devicemodel = parts[11].split(" ")[1];
			} else if (parts[11].contains("_")) {
				devicemodel = parts[11].split("_")[1];
			}
			
			String country = null;
			LocationBean bean = util.getLocationBean(parts[14].trim());
			if (bean != null) {
				country = bean.getCountryCode();
			}			
			
			String startDate = startTime.split(" ")[0];
			String endDate = endTime.split(" ")[0];
			
			if (startDate.equals(endDate)) { // Daily
				context.write(new Text(imei), new Text(devicemodel + "." + country + "." + startDate));
			} else { // Quarterly
				context.write(new Text(imei), new Text(devicemodel + "." + country + ".quarter"));
			}
		}
		
	}
}