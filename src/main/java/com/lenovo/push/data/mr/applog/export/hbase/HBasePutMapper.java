package com.lenovo.push.data.mr.applog.export.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Bulk import device tags to HBase
 * <p>
 * Parses device tags from files
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

  final static byte[] TAGS_COL_FAM = "dynamic_props".getBytes();
  final static byte[] TAGS_COL_NAME = "tags".getBytes();
  final static String FIELD_SEP = "\001";
  final static int NUM_FIELDS = 2;

  ImmutableBytesWritable hKey = new ImmutableBytesWritable();
  KeyValue kv;

  
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	String line = value.toString();
	
	// fields[0]: deviceid  fields[1]: tags
    String[] fields = line.split(FIELD_SEP);
    if (fields.length != NUM_FIELDS) {
      context.getCounter("HBasePutMapper", "INVALID_FIELD_LEN").increment(1);
      return;
    }
    hKey.set(fields[0].getBytes());
    Put put = new Put(hKey.copyBytes());
    put.add(TAGS_COL_FAM, TAGS_COL_NAME, fields[1].getBytes());

    context.write(hKey, put);
    context.getCounter("HBasePutMapper", "NUM_MSGS").increment(1);
  }
}
