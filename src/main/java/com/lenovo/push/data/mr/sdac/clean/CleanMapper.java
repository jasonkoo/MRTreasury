package com.lenovo.push.data.mr.sdac.clean;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.lenovo.push.data.ip2location.entity.LocationBean;
import com.lenovo.push.data.ip2location.util.IpConvertUtil;


/**
 * Input:
 * dbtbl,logtime,method,times,version,networkMode,v,imsi,imei,s,l,p,imsi2,c,ip,f,imei2,isRetry,systemID,cellID,latitude,longitude,province,city,district,coTime,cn,themonth,thedate
 * Output:
 * key: imei,logtime
 * value: dbtbl,logtime,method,times,version,networkMode,v,imsi,imei,s,l,p,imsi2,c,ip,f,imei2,isRetry,systemID,cellID,latitude,longitude,province,city,district,coTime,themonth,thedate,countrycode,logtime
 *
 */
public class CleanMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static String FIELD_SEP = "\001";
	private static String OUTPUT_SEP = "\002";
	private static String PAIR_SEP = "\003";
	private IpConvertUtil util;	
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		this.util = IpConvertUtil.getSingleInstance();	
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] parts = line.split(FIELD_SEP, -1);
		if (parts.length == 29 ){
			if (parts[14].trim().length() >= 7) {
				String imei = parts[8];
				String logtime = parts[1];
				String countrycode = null;
				LocationBean locationBean = util.getLocationBean(parts[14]);
				if (locationBean != null) {
					countrycode = locationBean.getCountryCode();
				} else {
					context.getCounter("SDAC_CLEAN_MAPPER", "IP_PARSE_FAILURE_LINES").increment(1);
				}
				context.write(new Text(imei + PAIR_SEP + logtime), new Text(line + FIELD_SEP + countrycode + OUTPUT_SEP + logtime));
			} else {
				String imei = parts[8];
				String logtime = parts[1];
				context.write(new Text(imei + PAIR_SEP + logtime), new Text(line + FIELD_SEP + "null" + OUTPUT_SEP + logtime));
				context.getCounter("SDAC_CLEAN_MAPPER", "INVALID_IP_LEN_LINES").increment(1);
			}
		} else {
			 context.getCounter("SDAC_CLEAN_MAPPER", "INVALID_FIELD_LEN_LINES").increment(1);
		}
		context.getCounter("SDAC_CLEAN_MAPPER", "TOTAL_INPUT_LINES").increment(1);
	}
}