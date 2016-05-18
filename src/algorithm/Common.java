package algorithm;

import java.util.List;

import utils.MP;

public class Common {
	
	public static double getListAverage(List<Double> list){
		 int num = list.size();
		 double sum = 0;
		 for(int i = 0;i < num;i++){
			 sum += list.get(i);
		 }
		 MP.logln("sum is " + sum, false);
		 return (double)(sum / num);
	 }

	//求标准差
	//1. 先求和均值差的平方和
	//2. 除N
	//3. 开方
	public static double getListStandardDevition(List<Double> list, double average){
		int num = list.size();
        double sum = 0;
        for(int i = 0;i < num;i++){
        	sum += Math.pow(list.get(i) - average, 2);
        }
        return Math.sqrt(sum / num);
    }
	
	public static double maxOfArray(double[] array){
		if (array.length == 0){
			return 0;
		}
		double max = array[0];
		for (int i = 0; i < array.length; i++){
			max = Math.max(max, array[i]);
		}
		MP.logln("max is: " + max, false);
		return max;
	}
	
	//过滤奇异点
	public static List<Double> removeBadPoints(List<Double> valueList){
		double tempAverage = Common.getListAverage(valueList);
		double sigma = Common.getListStandardDevition(valueList, tempAverage);
		//踢除奇异点
		//a.第一行特殊处理
		if ( Math.abs(valueList.get(0) - tempAverage)  >= 3*sigma ){
			valueList.set(0, tempAverage);
		}
		//b.其他行数
		for (int i = 1; i < valueList.size(); i++){
			if ( Math.abs(valueList.get(i) - tempAverage)  >= 3*sigma ){
				valueList.set(i, valueList.get(i-1));
			}
		}
		return valueList;
	}

}
