package com.lenovo.push.data.mr.applog.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * input<br>
 * user,user对标签1的权重,user对标签2的权重,...,总计数<br>
 * 
 * @author hanxiaoten
 *
 */
public class InitUserScore {

	private static final String INPUT = "input";
	private static final String OUTPUT = "output";
	private static final Float[] initScore = { 1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f,
			1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f,
			1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f,
			1.0f, 1.0f, 1.0f, 1.0f, 1.0f };

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		new GenericOptionsParser(conf, args);
		String input = conf.get(INPUT);
		String output = conf.get(OUTPUT);
		
		System.out.println("--------------------------------------------------");
	    System.out.println("Arguments passed in: ");
	    System.out.println("input: " + input);
	    System.out.println("output: " + output);
	    System.out.println("--------------------------------------------------");

		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream out = fs.create(new Path(output));
		Path in = new Path(input);
		FileStatus status[] = fs.listStatus(in);
		for (FileStatus statusx : status) {
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(statusx.getPath())));
			String line = null;
			int count = 0;
			while ((line = br.readLine()) != null) {
				int first = line.indexOf(",");
				int last = line.lastIndexOf(",");
				out.writeUTF(line.substring(0, first) + "," + minus(initScore, line.substring(first + 1, last)) + "\n");
				if (count++ % 10000 == 0) {
					out.flush();
				}
			}
			
			br.close();
		}
		if (out != null) {
			out.close();
		}
		fs.close();
	}

	private static String minus(Float[] initScore, String line) {
		StringBuffer newScore = new  StringBuffer();
		String[] ss = line.split(",");
		float t0 = initScore[0] - Float.parseFloat(ss[0]);
		if (t0 < 0) {
			newScore.append("0.0");
		} else {
			newScore.append(t0);
		}
		for (int i = 1; i < ss.length; i++) {
			float t = initScore[i] - Float.parseFloat(ss[i]);
			if (t < 0) {
				newScore.append(",0.0");
			} else {
				newScore.append("," + t);
			}
		}
		return newScore.toString();
	}

}
