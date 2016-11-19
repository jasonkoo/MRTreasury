package com.lenovo.push.data.mr.util;

public class CountryCodeUtil {
	
	public static String normalize(String countryCode) {
		String result = countryCode;
		if (countryCode != null && countryCode.startsWith("CN_") && ChineseUtil.containsChinese(countryCode)) {
			result = countryCode.substring(countryCode.indexOf("_") + 1);
		}
		return result;
	}
	
	public static void main(String[] args) {
		String countryCode = null;
		System.out.println(CountryCodeUtil.normalize(countryCode));

	}

}
