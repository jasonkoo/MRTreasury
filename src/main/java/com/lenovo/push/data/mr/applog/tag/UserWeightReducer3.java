package com.lenovo.push.data.mr.applog.tag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserWeightReducer3 extends Reducer<Text, Text, Text, Text> {
	
	public static int varNum = 33;
	private static String[] labels = { "考试学习", "聊天社交", "理财金融", "短信通讯", "电子书",
			"系统优化", "音乐铃声", "生活购物", "健康健美", "棋牌桌游", "其他游戏", "射击飞行", "体感动作",
			"角色策略", "益智休闲", "图册漫画", "实用工具", "经营养成", "音乐节奏", "地图导航", "新闻阅读",
			"其他应用", "桌面美化", "儿童教育", "影音视频", "商务职场", "天气日历", "体育竞速", "办公效率",
			"塔防跑酷", "网游", "拍摄美图", "移植汉化" };

	@Override
	public void reduce(Text key, Iterable<Text> valueSet, Context context) throws IOException, InterruptedException {
		Map<String, Float> map = new HashMap<String, Float>();
		for (Text value : valueSet) {
			String[] segs = value.toString().split(",");
			int index = 0;
			for (String seg : segs) {
				if (Float.parseFloat(seg) != 0.0) {
					break;
				}
				index++;
			}
			if (index == segs.length) {
				context.write(key, new Text("无"));
				return;
			} else {
				map.put(labels[index], Float.parseFloat(segs[index]));
				for (int i = index + 1; i < segs.length; i++) {
					if (Float.parseFloat(segs[i]) != 0.0) {
						map.put(labels[i], Float.parseFloat(segs[i]));
					}
				}
			}
		}
		
		// 汇总游戏类别
		String game_options = sumGameSore(map);
		String[] go = null;
		if (game_options != null) {
			go = game_options.split("\u0001");
			map.put("游戏", Float.parseFloat(go[0]));
		}
		
		// 排序
		List<Map.Entry<String, Float>> list = new ArrayList<Map.Entry<String, Float>>(map.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<String, Float>>() {
			public int compare(Entry<String, Float> o1, Entry<String, Float> o2) {
				return (int)(o2.getValue() * 1000 - o1.getValue() * 1000);
			}
		});
		StringBuffer sb = new StringBuffer();
		if (list.get(0).getKey().equals("游戏")) {
			sb.append(list.get(0).getKey() + ":" + list.get(0).getValue() + go[1]);
		} else {
			sb.append(list.get(0).getKey() + ":" + list.get(0).getValue());
		}
		for (int k = 1; k < list.size(); k++) {
			if (list.get(k).getKey().equals("游戏")) {
				sb.append("," + list.get(k).getKey() + ":" + list.get(k).getValue() + go[1]);
			} else {
				sb.append("," + list.get(k).getKey() + ":" + list.get(k).getValue());
			}
		}
		context.write(key, new Text(sb.toString()));
	}

	/**
	 * 汇总游戏大类，细分作为附属项
	 * 
	 * @param map
	 * @return 游戏总分\u0001(单项1:分数1,单项2:分数2,...,)
	 */
	private String sumGameSore(Map<String, Float> map) {
		float game_score = 0.0f;
		Map<String, Float> game_map = new HashMap<String, Float>();
		if (map.containsKey("射击飞行")) {
			game_score += map.get("射击飞行");
			game_map.put("射击飞行", map.get("射击飞行"));
			map.remove("射击飞行");
		} 
		if (map.containsKey("棋牌桌游")) {
			game_score += map.get("棋牌桌游");
			game_map.put("棋牌桌游", map.get("棋牌桌游"));
			map.remove("棋牌桌游");
		}
		if (map.containsKey("体感动作")) {
			game_score += map.get("体感动作");
			game_map.put("体感动作", map.get("体感动作"));
			map.remove("体感动作");
		}
		if (map.containsKey("角色策略")) {
			game_score += map.get("角色策略");
			game_map.put("角色策略", map.get("角色策略"));
			map.remove("角色策略");
		}
		if (map.containsKey("益智休闲")) {
			game_score += map.get("益智休闲");
			game_map.put("益智休闲", map.get("益智休闲"));
			map.remove("益智休闲");
		}
		if (map.containsKey("经营养成")) {
			game_score += map.get("经营养成");
			game_map.put("经营养成", map.get("经营养成"));
			map.remove("经营养成");
		}
		if (map.containsKey("音乐节奏")) {
			game_score += map.get("音乐节奏");
			game_map.put("音乐节奏", map.get("音乐节奏"));
			map.remove("音乐节奏");
		}
		if (map.containsKey("体育竞速")) {
			game_score += map.get("体育竞速");
			game_map.put("体育竞速", map.get("体育竞速"));
			map.remove("体育竞速");
		}
		if (map.containsKey("塔防跑酷")) {
			game_score += map.get("塔防跑酷");
			game_map.put("塔防跑酷", map.get("塔防跑酷"));
			map.remove("塔防跑酷");
		}
		if (map.containsKey("网游")) {
			game_score += map.get("网游");
			game_map.put("网游", map.get("网游"));
			map.remove("网游");
		}
		if (map.containsKey("移植汉化")) {
			game_score += map.get("移植汉化");
			game_map.put("移植汉化", map.get("移植汉化"));
			map.remove("移植汉化");
		}
		if (map.containsKey("其他游戏")) {
			game_score += map.get("其他游戏");
			game_map.put("其他游戏", map.get("其他游戏"));
			map.remove("其他游戏");
		}
		if (game_map.size() != 0) {
			List<Map.Entry<String, Float>> game_list = new ArrayList<Map.Entry<String, Float>>(game_map.entrySet());
			Collections.sort(game_list, new Comparator<Map.Entry<String, Float>>() {
				public int compare(Entry<String, Float> o1, Entry<String, Float> o2) {
					return (int)(o2.getValue() * 1000 - o1.getValue() * 1000);
				}
			});
			StringBuffer game_sb = new StringBuffer();
			game_sb.append("(" + game_list.get(0).getKey() + ":" + game_list.get(0).getValue());
			for (int k = 1; k < game_list.size(); k++) {
				game_sb.append("," + game_list.get(k).getKey() + ":" + game_list.get(k).getValue());
			}
			game_sb.append(")");
			return game_score + "\u0001" + game_sb.toString();
		} else {
			return null;
		}
	}
	
	public static void main(String[] args) {
		System.out.println(Float.parseFloat("-0.008269783"));
	}

}
