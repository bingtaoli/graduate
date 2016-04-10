package rms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.MyString;

public class CSVReducer extends Reducer<Text, DoubleWritable, Text, Text> {
	
	//private DoubleWritable result = new DoubleWritable();
	private Text resultArrayString = new Text();
	public static double costMapperTime = 0;

	protected void finalize(){
		System.out.println(">>>>>>>>>end of reducer");
	}

	public double getListAverage(List<Double> list){
		 int num = list.size();
		 int sum = 0;
		 for(int i = 0;i < num;i++){
           sum += list.get(i);
		 }
		 return (double)(sum / num);
	 }
	
	//先求和均值差的平方和
	//除N
	//开方
	public double getListStandardDevition(List<Double> list, double average){
		int num = list.size();
        double sum = 0;
        for(int i = 0;i < num;i++){
        	sum += Math.pow(list.get(i) - average, 2);
        }
        return Math.sqrt(sum / num);
    }
	
	//输入是时间--每一行倒数第二列的double值
	public void reduce(Text key, Iterable<DoubleWritable> values,  Context context) 
			throws IOException, InterruptedException {
		double MAX = 0;
		double MIN = 0;
		double RMS = 0;
		
		//转换成List
		List<Double> valueList = new ArrayList<>();
		for (DoubleWritable val : values) {
			valueList.add(val.get());
		}
		double tempAverage = getListAverage(valueList);
		double sigma = getListStandardDevition(valueList, tempAverage);
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
		//更新均值
		double AVERAGE = getListAverage(valueList);
		//均方根
		//a.平方和
		for (int i = 0; i < valueList.size(); i++){
			RMS += Math.pow(valueList.get(i), 2);
		}
		//b.均值
		RMS = RMS / valueList.size();
		//c.开方
		RMS = Math.sqrt(RMS);
		//峰峰值
		for (int i = 0; i < valueList.size(); i++){
			if (valueList.get(i) > MAX){
				MAX = valueList.get(i);
			}
			if (valueList.get(i) < MIN){
				MIN = valueList.get(i);
			}
		}
		double XPP = MAX - MIN;
		//波形指数
		double SF = 0;
		if (AVERAGE != 0){
			//avoid infinity
			SF  = RMS / Math.abs(AVERAGE); 
		}
		//峰值指标
		double CF = MAX / RMS;
		//脉冲指标
		double IF = 0;
		if (AVERAGE != 0){
			 IF = RMS / AVERAGE;
		}
		//裕度因子
		double XR = 0;
		for (int i = 0; i < valueList.size(); i++){
			XR += Math.sqrt(Math.abs(valueList.get(i)));
		}
		XR = XR / valueList.size();
		XR = Math.pow(XR, 2);
		double CLF = MAX / XR;
		//峭度因子
		double BEIDA = 0;
		for (int i = 0; i < valueList.size(); i++){
			BEIDA += Math.pow(valueList.get(i), 4);
		}
		BEIDA = BEIDA / valueList.size();
		double KV = BEIDA / Math.pow(RMS, 4);
		
		//NOTE 现在的顺序决定了第二个job的获取数据顺序，不能随便更改!
		String join = MyString.join(", ", String.valueOf(RMS), String.valueOf(XPP), String.valueOf(SF), 
				String.valueOf(CF), String.valueOf(IF), String.valueOf(CLF), String.valueOf(KV));
		resultArrayString.set(join);
	    context.write(key, resultArrayString);
	}
	
}
