package utils;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * still ugly code, forgive me
 * using ";" to split second dimension of an array
 * @author matthew
 *
 */
public class ArrayToStr {
	
	public static String encodeTwoDimensionArray(double[][] array){
		
		String s = "";
		String[] arrayStrings = new String[array.length];
		
		for (int i = 0; i < array.length; i++){
			String temp = Arrays.toString(array[i]);
			arrayStrings[i] = temp;
		}
		
		s = String.join(";", arrayStrings);
		
		return s;
	}
	
	public static double[][] decodeTwoDimensionArray(String s){
		
		ArrayList<double[]> list = new ArrayList<>();
		
		String[] ss = s.split(";");
		
		for (int i = 0; i < ss.length; i++){
			//[1, 2, 3]
			String temp = ss[i];
			temp = temp.replace("[", "").replace("]", "");
			
			String[] tempArray = temp.split(",");
			double[] doubleArray = new double[tempArray.length];
			for (int j = 0; j < tempArray.length; j++){
				doubleArray[j] = Double.parseDouble(tempArray[j]); 
			}
			list.add(doubleArray);
		}
		
		double[][] result = new double[list.size()][];
		for (int i = 0; i < list.size(); i++){
			result[i] = list.get(i);
		}
		
		return result;
	}

}
