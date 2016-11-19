package com.lenovo.push.data.mr.applog.extract;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Input:
 * deviceId,device对标签1的权重,device对标签2的权重,...,总计数<br><br>
 * Output:
 * key: NullWritable
 * value: deviceid\001tag1,tag2,tag3...
 * 
 * @author gulei2
 *
 */
public class ExtractMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	public static int varNum = 33;
	private static String[] labels = { "考试学习", "聊天社交", "理财金融", "短信通讯", "电子书",
			"系统优化", "音乐铃声", "生活购物", "健康健美", "棋牌桌游", "其他游戏", "射击飞行", "体感动作",
			"角色策略", "益智休闲", "图册漫画", "实用工具", "经营养成", "音乐节奏", "地图导航", "新闻阅读",
			"其他应用", "桌面美化", "儿童教育", "影音视频", "商务职场", "天气日历", "体育竞速", "办公效率",
			"塔防跑酷", "网游", "拍摄美图", "移植汉化" };
	
	private double threshold;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		this.threshold = conf.getDouble("threshold", 0.1);
	}
	
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] parts = value.toString().split(",");
		if (parts.length == varNum + 2) {
			StringBuilder sb = new StringBuilder();
			sb.append(parts[0]);
			sb.append("\001");
			for (int i = 1; i <= varNum; i++) {
				if(Double.parseDouble(parts[i]) >= threshold) {
					sb.append(labels[i - 1]);
					sb.append(",");
				}
			}
			sb.deleteCharAt(sb.length() - 1);
			context.write(NullWritable.get(), new Text(sb.toString()));
		}
		
		
	}
}
