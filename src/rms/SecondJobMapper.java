package rms;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondJobMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>  {
	
	/**
	 * 归一化数组数据
	 * TODO 可以直接改变参数，不用返回
	 * @param arr
	 * @return
	 */
	public static double[] normalization(double[] arr){
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
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
	  throws IOException, InterruptedException {
		//数据归一化
		// a. 分隔一行
		String s = value.toString();
		String[] values = s.split(",");
		double[] arr = new double[values.length - 1];
		// b.转换成double数组
		for (int i = 0; i < values.length - 1; i++){
			// 数据从第二列开始读,第一列是时间
			arr[i] = Double.parseDouble(values[i+1]);
		}
		arr = normalization(arr);
		//TODO 主成分分析
		System.out.println(value);
	}
	
}
