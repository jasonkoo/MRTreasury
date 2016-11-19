package com.lenovo.push.data.mr.sdac.export.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Bulk import sdac device imeis to HBase
 * <p>
 * Parses sdac device imeis from files
 * <ImmutableBytesWritable, Put>.
 * <p>
 * The ImmutableBytesWritable key is used by the TotalOrderPartitioner to map it
 * into the correct HBase table region.
 * <p>
 * The Put value holds the HBase mutation information (column family,
 * column, and value)
 */
public class HBasePutMapper extends
    Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

  final static byte[] COL_FAMILY_NAME = "device_info".getBytes();
  final static byte[] COL_NAME = "occupying".getBytes();
  final static String DUMMY_STRING = "";

  ImmutableBytesWritable hKey = new ImmutableBytesWritable();
  
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  String imei = value.toString().trim();
	  if (imei.length() == 0 || imei.contains("null") || imei.length() <= 4) {
		  context.getCounter("HBasePutMapper", "INVALID_FIELD_LEN").increment(1);
		  return;
	  }
	  
	  hKey.set(imei.getBytes());
	  Put put = new Put(hKey.copyBytes());
	  put.add(COL_FAMILY_NAME, COL_NAME, DUMMY_STRING.getBytes());
	  
	  context.write(hKey, put);
	  context.getCounter("HBasePutMapper", "NUM_MSGS").increment(1);
  }
}
