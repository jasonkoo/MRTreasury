package com.lenovo.push.data.mr.applog.export.solr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.lenovo.czlib.nodex.conf.ZKProperties;
import com.lenovo.lps.push.common.eventbus.PushEventBus;


/**
 * Input:
 * deviceId,device对标签1的权重,device对标签2的权重,...,总计数<br><br>
 * Output:
 * null
 * 
 * @author gulei2
 *
 */
public class ExportMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
	public static int varNum = 33;
	private static String[] labels = { "考试学习", "聊天社交", "理财金融", "短信通讯", "电子书",
			"系统优化", "音乐铃声", "生活购物", "健康健美", "棋牌桌游", "其他游戏", "射击飞行", "体感动作",
			"角色策略", "益智休闲", "图册漫画", "实用工具", "经营养成", "音乐节奏", "地图导航", "新闻阅读",
			"其他应用", "桌面美化", "儿童教育", "影音视频", "商务职场", "天气日历", "体育竞速", "办公效率",
			"塔防跑酷", "网游", "拍摄美图", "移植汉化" };
	
	private PushEventBus DATA_EVENT_BUS = null;
	
	private double threshold = 0;
	private String topicName = null;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		System.setProperty("envType", conf.get("envType"));
		this.threshold = conf.getDouble("threshold", 0.1);
		this.topicName = conf.get("topicName");
		this.DATA_EVENT_BUS = new PushEventBus(new ZKProperties(new String[] { "/data/eventbus" }, false));
	}
	
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] part = value.toString().split(",");
		if (part.length != varNum + 2) return;
		StringBuffer sb = new StringBuffer();
		sb.append("data.tags");
		sb.append("\001");
		sb.append("\001");
		sb.append(part[0]);
		sb.append("\001");
		sb.append("\001");
		sb.append("\001");
		sb.append("\001");
		sb.append("\001");
		sb.append("\001");
		for (int i = 1; i <= varNum; i++) {
			if(Double.parseDouble(part[i]) >= threshold) {
				sb.append(labels[i - 1]);
				sb.append(",");
			}
		}
		sb.deleteCharAt(sb.length() - 1);
		
		DATA_EVENT_BUS.publish(topicName, sb.toString().getBytes());
	}
}
