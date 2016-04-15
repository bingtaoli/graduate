package algorithm;

import java.util.List;

import utils.MP;

public class Average {
	
	public static double getListAverage(List<Double> list){
		 int num = list.size();
		 double sum = 0;
		 for(int i = 0;i < num;i++){
			 sum += list.get(i);
		 }
		 MP.println("sum is " + sum);
		 return (double)(sum / num);
	 }

}
