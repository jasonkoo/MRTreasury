package com.lenovo.push.data.mr.applog.io;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyComparator extends WritableComparator {

	protected KeyComparator() {
		super(TextPair.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		TextPair tp1 = (TextPair) w1;
		TextPair tp2 = (TextPair) w2;
		
		int cmp = tp1.getFirst().compareTo(tp2.getFirst());
		if (cmp != 0) {
			return cmp;
		}
		return -tp1.getSecond().compareTo(tp2.getSecond()); // reverse
	}
	
	public static void main(String[] args) {
		
		TextPair tp1 = new TextPair("com.meitu.meipaipc", "0.5");
		TextPair tp2 = new TextPair("com.meitu.meipaipc", "");
		
		KeyComparator kc = new KeyComparator();
		
		System.out.println(kc.compare(tp1, tp2));
	}
}
