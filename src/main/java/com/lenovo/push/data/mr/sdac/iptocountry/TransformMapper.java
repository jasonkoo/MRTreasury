package com.lenovo.push.data.mr.sdac.iptocountry;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.lenovo.push.data.ip2location.entity.LocationBean;
import com.lenovo.push.data.ip2location.util.IpConvertUtil;


/**
 * Input:
 * 
 * Output:
 * imei\001country\001devicemodel\001logtime
 *
 */
public class TransformMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private static String FIELD_SEP = "\001";
	private IpConvertUtil util;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		this.util = IpConvertUtil.getSingleInstance();	
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] parts = line.split(FIELD_SEP, -1);
		if (parts.length == 26){
			String ip = parts[14].trim();
			if (ip.length() > 3) {
				LocationBean bean = util.getLocationBean(ip);
				if (bean != null) {
					context.write(NullWritable.get(), new Text(parts[8] + FIELD_SEP + bean.getCountry() + FIELD_SEP + parts[11] + FIELD_SEP + parts[1]));
				}			
			}		
		}
		
	}
	
	
	public static void main(String[] args) {
		String ip = "1.187.255.233";
		IpConvertUtil util = IpConvertUtil.getSingleInstance();
		LocationBean bean = util.getLocationBean(ip);
		if (bean != null) {
			System.out.println(bean.getCountry());
		}
		
	}
}