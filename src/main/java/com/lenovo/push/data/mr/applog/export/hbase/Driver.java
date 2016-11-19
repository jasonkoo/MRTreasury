package com.lenovo.push.data.mr.applog.export.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * HBase bulk import<br>
 * Data preparation MapReduce job driver
 * -DinputPath -DoutputPath -DtableName
 * 
 */
public class Driver extends Configured implements Tool {
	
	private static final String INPUTPATH = "inputPath";
	private static final String OUTPUTPATH = "outputPath";
	private static final String TABLENAME = "tableName";
	
	public int run(String[] arg0) throws Exception {
		Configuration conf = this.getConf();
		String inputPath = conf.get(INPUTPATH);
		String outputPath = conf.get(OUTPUTPATH);
		String tableName = conf.get(TABLENAME);
		
		System.out.println("inputPath: " + inputPath);
		System.out.println("outputPath: " + outputPath);
		System.out.println("tableName: " + tableName);
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(outputPath))) {
			fs.delete(new Path(outputPath), true);
		}   	
		
	    Job job = Job.getInstance(conf);
	    job.setJobName("Bulk Import Device Tags From HDFS to HBase");
	    job.setJarByClass(Driver.class);
	    
	    FileInputFormat.setInputPaths(job, new Path(inputPath));
	    FileOutputFormat.setOutputPath(job, new Path(outputPath));
	    
	    job.setMapperClass(HBasePutMapper.class);
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    
	    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
	    job.setMapOutputValueClass(Put.class);

	    // Load hbase-site.xml 
	    HBaseConfiguration.addHbaseResources(conf);
	    HTable hTable = new HTable(conf, tableName);
	    
	    // Auto configure partitioner and reducer
	    HFileOutputFormat2.configureIncrementalLoad(job, hTable);

	    return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Driver(), args);
		System.exit(res);
	}
}
