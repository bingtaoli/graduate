package rms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CSVReducer extends Reducer<Text, DoubleWritable, Text, Text> {
	
	//private DoubleWritable result = new DoubleWritable();
	private Text resultArrayString = new Text();
	public static double costMapperTime = 0;
	
	public CSVReducer() {
		// TODO Auto-generated constructor stub
	}

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
	
	public double getListStandardDevition(List<Double> list, double average){
		int num = list.size();
        double sum = 0;
        for(int i = 0;i < num;i++){
            sum += Math.sqrt(((double)list.get(i) - average) * (list.get(i) -average));
        }
        return (sum / (num - 1));
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
		for (int i = 0; i < valueList.size(); i++){
			if ( Math.abs(valueList.get(i) - tempAverage)  >= 3*sigma ){
				if (i < 2){
					valueList.set(i, tempAverage);
				} else {
					valueList.set(i, valueList.get(i-1));
				}
			}
		}
		
		//均方根
		//1.1平方和
		for (int i = 0; i < valueList.size(); i++){
			RMS += Math.pow(valueList.get(i), 2);
		}
		//1.2均值
		RMS = RMS / valueList.size();
		//1.3开方
		RMS = Math.sqrt(RMS);
		//2. 峰峰值
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
		double AVERAGE = getListAverage(valueList);
		double SF  = RMS / Math.abs(AVERAGE); 
		//峰值指标
		double CF = MAX / MIN;
		//脉冲指标
		double IF = RMS / AVERAGE;
		//裕度因子
		double XR = 0;
		for (int i = 0; i < valueList.size(); i++){
			XR += Math.sqrt(valueList.get(i));
		}
		XR = XR / valueList.size();
		XR = Math.pow(XR, 2);
		double CLF = MAX / XR;
		//峭度因子
		double BEIDA = 0;
		for (int i = 0; i < valueList.size(); i++){
			BEIDA += Math.pow(valueList.get(i), 4);
		}
		BEIDA = BEIDA / 4;
		double KV = BEIDA / Math.pow(RMS, 4);
		
		String join = String.join(", ", String.valueOf(RMS), String.valueOf(XPP), String.valueOf(SF), 
				String.valueOf(CF),String.valueOf(IF), String.valueOf(CLF),String.valueOf(KV));
		resultArrayString.set(join);
		
	    //result.set(RMS);
	    context.write(key, resultArrayString);
	}
	
}
