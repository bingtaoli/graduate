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

}
