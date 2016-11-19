package com.lenovo.push.data.mr.device.importx;

import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.lenovo.czlib.nodex.conf.ZKProperties;
import com.lenovo.push.bigdata.devicesdk.DeviceManager;
import com.lenovo.push.bigdata.devicesdk.entity.Device;
import com.lenovo.push.data.mr.util.CountryCodeUtil;


/**
 * Input:
 * key: pid
 * value: NullWritable
 * Output:
 * key: NullWritable
 * value: pid\001deviceid\001devicemodel\001countrycode\001cityname\001peversion\001pevercode\001operationtype\001deviceidtype\001channelname\001custversion\001imsi\001osversion\001pepkg\001pollversion\001updatetime
 * 
 * @author gulei2
 *
 */
public class ImportReducer extends Reducer<Text, NullWritable, NullWritable, Text> {
	
	private DeviceManager dm;
	private static String FIELD_SEP = "\001";
	//private static String COLLECTION_SEP = ",";
	//private static String MAP_SEP = ":";
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		System.setProperty("envType", conf.get("envType"));
		
		String part = conf.get("part");
		if (part.equals("domestic")) {
			this.dm = DeviceManager.getDefaultInstance();
		} else if (part.equals("overseas")) {
			this.dm = DeviceManager.getInstance(new ZKProperties(new String[]{"/data/prw_mongo"}));
		}
		
	}
	
	@Override
	public void reduce(Text key, Iterable<NullWritable> valueSet, Context context) throws IOException, InterruptedException {
		String pid = key.toString();
		try {
			Device device = dm.findOne(Device.class, pid);
			if (device != null) {
				context.write(NullWritable.get(), new Text(serialize(device)));
			}
		} catch (Exception e) {			
		}		
	}
	
	private String serialize(Device device) {
		
		StringBuffer sb = new StringBuffer();
		sb.append(device.get_id());
		sb.append(FIELD_SEP);
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
		sb.append(FIELD_SEP);
		Date d = new Date(device.getUpdateTime());
		sb.append(sdf.format(d));
		/*Set<String> sids = device.getSids();
		if (sids != null && sids.size() > 0) {
			for (String sid : sids) {
				sb.append(sid);
				sb.append(COLLECTION_SEP);
			}
			sb.deleteCharAt(sb.length() - 1);
		}		
		
		sb.append(FIELD_SEP);		
		HashMap<String, String> prpts = device.getProperties();
		if (prpts != null && prpts.size()>0) {
			for (String key : prpts.keySet()) {			
				sb.append(key);
				sb.append(MAP_SEP);
				sb.append(prpts.get(key));
				sb.append(COLLECTION_SEP);
			}
			sb.deleteCharAt(sb.length() - 1);
		}
		*/
		
		return sb.toString();
	}
}
