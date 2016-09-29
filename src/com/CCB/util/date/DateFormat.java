package com.CCB.util.date;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class DateFormat {

	public static String getDateTime(String dateString){
		TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		Date date;
		try {
			date = sdf.parse(dateString+" 00:00:00");
			sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"); 
			return sdf.format(date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("time convert failure");
		}
		return "*";
	}

	
	
	public static String getLocalDateTime(Date wORK_DATE){
//		TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		String date = sdf.format(wORK_DATE);
		return date;
	}
	
	

}
