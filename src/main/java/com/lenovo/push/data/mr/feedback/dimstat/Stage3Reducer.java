package com.lenovo.push.data.mr.feedback.dimstat;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Input: dimname<adid\001dimval\001thedate -> [eventname\tsum,...,eventname\tsum]
 * Output: /dimname
 *         adid\001dimval\001thedate\001pushsum\001arrivesum\001displaysum\001clicksum\001downloadsum\001installsum\001activatesum\001downloadfailuresum\001installfailuresum
 * @author gulei2
 *
 */

public class Stage3Reducer extends Reducer<Text, Text, Text, NullWritable> {
	
	private MultipleOutputs<Text, NullWritable> mos;
	private HashMap<String, Integer> eventOrderPair = new HashMap<String, Integer>() {
		private static final long serialVersionUID = 1L;
		{
			put("push", 0);
			put("arrive", 1);
			put("display", 2);
			put("click", 3);
			put("download", 4);
			put("install", 5);
			put("activate", 6);
			put("downloadfail", 7);
			put("installfail", 8);
		}
	};
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<Text, NullWritable>(context);
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> vals, Context context) throws IOException, InterruptedException {
		String[] parts = key.toString().split("<");
		int[] stats = new int[eventOrderPair.size()];
		
		if (parts.length == 2) {
			StringBuffer sb = new StringBuffer();
			sb.append(parts[1]);
			sb.append("\001");
			
			for (Text val : vals) {
				String[] eventSumPair = val.toString().split("\t");
				if (eventSumPair.length == 2) {
					String eventName = eventSumPair[0];
					int index = eventOrderPair.get(eventName);
					stats[index] = Integer.parseInt(eventSumPair[1]);
				}
			}
			
			for (int i = 0; i < stats.length; i++) {
				sb.append(stats[i]);
				sb.append("\001");
			}
			//sb.deleteCharAt(sb.length() - 1);
			//sb.append("NULL");
			mos.write(new Text(sb.toString()), NullWritable.get(), parts[0] + "/part");
 		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
}
