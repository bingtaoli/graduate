package algorithm;

import java.util.List;

public class Normalization {
	
	/**
	 * 归一化数组数据
	 * TODO 可以直接改变参数，不用返回
	 * @param arr
	 * @return
	 */
	public static double[] normalizationArray(double[] arr){
		double squaresSum = 0;
		for (int i = 0; i < arr.length; i++){
			squaresSum += Math.pow(arr[i], 2);
		}
		if (squaresSum == 0){
			return arr;
		}
		for (int i = 0; i < arr.length; i++){
			arr[i] = Math.pow(arr[i], 2) / squaresSum;
		}
		return arr;
	}
	
	public static List<Double> normalizatioList(List<Double> arr){
		double squaresSum = 0;
		for (int i = 0; i < arr.size(); i++){
			squaresSum += Math.pow(arr.get(i), 2);
		}
		if (squaresSum == 0){
			return arr;
		}
		for (int i = 0; i < arr.size(); i++){
			arr.set(i, Math.pow(arr.get(i), 2) / squaresSum);
		}
		return arr;
	}
	
}
