package rms;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class MyTimer {
	
	private static Map<String, Double> BenchMap = new HashMap<>(); 
	
	public static double getNowMills(){
		Date now = new Date();
		return now.getTime();
	}
	
	public static void setMark(String k){
		Date now = new Date();
		BenchMap.put(k, (double) now.getTime());
	}
	
	public static double getCost(String k){
		if (BenchMap.get(k) != null){
			double start = BenchMap.get(k);
			Date now = new Date();
			double end = now.getTime();
			return end - start;
		}
		return -1;
	}
}
