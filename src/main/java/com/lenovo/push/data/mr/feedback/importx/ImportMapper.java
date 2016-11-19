package com.lenovo.push.data.mr.feedback.importx;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.lenovo.czlib.nodex.conf.ZKProperties;
import com.lenovo.push.bigdata.devicesdk.DeviceManager;
import com.lenovo.push.bigdata.devicesdk.entity.Device;
import com.lenovo.push.data.mr.util.CountryCodeUtil;


/**
 * Input:
 * dbtbl\001logtime\001pid\001bizType\001eventName\001feedbackId\001success\001sid\001errCode\001packName\001currVer\001targetVer\001value\001stepId
 * Output:
 * dbtbl\001logtime\001pid\001bizType\001eventName\001feedbackId\001success\001sid\001errCode\001packName\001currVer\001targetVer\001value\001stepId
 * \001deviceid\001devicemodel\001countrycode\001cityname\001peversion\001pevercode\001operationtype\001deviceidtype\001channelname\001custversion\001imsi\001osversion\001pepkg\001pollversion
 * @author gulei2
 *
 */
public class ImportMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private static String FIELD_SEP = "\001";
	private DeviceManager domesticDM;
	private DeviceManager overseasDM;
	private DeviceManager zukDM;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String envType = conf.get("envType");
		System.setProperty("envType", envType);
		
		this.domesticDM = DeviceManager.getInstance(new ZKProperties(new String[]{"/data/common"}));
		this.overseasDM = DeviceManager.getInstance(new ZKProperties(new String[]{"/data/prw_mongo"}));
		this.zukDM = DeviceManager.getInstance(new ZKProperties(new String[]{"/data/zuk_mongo"}));

	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] parts = line.split(FIELD_SEP);
		if (parts[5].startsWith("rinter2_")) {
			String dbtbl = parts[0];
			String pid = parts[2];
			if (dbtbl.startsWith("domestic")) { // starts with domestic
				Device device = domesticDM.findOne(Device.class, pid);
				if (device != null) {
					context.write(NullWritable.get(), new Text(line + FIELD_SEP + serialize(device)));
				}
			} else if (dbtbl.startsWith("overseas")) { // starts with overseas
				Device device = overseasDM.findOne(Device.class, pid);
				if (device != null) {
					context.write(NullWritable.get(), new Text(line + FIELD_SEP + serialize(device)));
				}
			} else if (dbtbl.startsWith("zuk")){ // starts with zuk
				Device device = zukDM.findOne(Device.class, pid);
				if (device != null) {
					context.write(NullWritable.get(), new Text(line + FIELD_SEP + serialize(device)));
				}				
			}
		}		
	}
	
	private String serialize(Device device) {		
		StringBuffer sb = new StringBuffer();
		sb.append(device.getDid());
		sb.append(FIELD_SEP);
		sb.append(device.getDeviceModel());
		sb.append(FIELD_SEP);
		sb.append(CountryCodeUtil.normalize(device.getCountryCode()));
		sb.append(FIELD_SEP);
		sb.append(device.getCityName());
		sb.append(FIELD_SEP);
		sb.append(device.getPeVer());
		sb.append(FIELD_SEP);
		sb.append(device.getPeVerc());
		sb.append(FIELD_SEP);
		sb.append(device.getOperationType());
		sb.append(FIELD_SEP);
		sb.append(device.getDidType());
		sb.append(FIELD_SEP);
		sb.append(device.getChannelName());			
		sb.append(FIELD_SEP);		
		sb.append(device.getCustVer());
		sb.append(FIELD_SEP);
		sb.append(device.getImsi());		
		sb.append(FIELD_SEP);
		sb.append(device.getOsVer());
		sb.append(FIELD_SEP);
		sb.append(device.getPePkg());		
		sb.append(FIELD_SEP);
		sb.append(device.getPollVer());		
		
		return sb.toString();
	}
}
