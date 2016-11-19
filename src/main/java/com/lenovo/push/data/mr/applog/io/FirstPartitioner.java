package com.lenovo.push.data.mr.applog.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FirstPartitioner extends Partitioner<TextPair, Text> {

	@Override
	public int getPartition(TextPair key, Text value, int numPartitions) {
		return Math.abs(key.getFirst().hashCode()) % numPartitions;		
	}
	
}
