package rms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import algorithm.Common;
import utils.MyString;

/**
 * 求时间的7个特征值
 * 传入的是一行代表一个csv
 * @author matthew
 */
public class TimeEigenMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text resultArrayString = new Text();
	private Text outKey = new Text();
	
	//key是行号，value是一行，代表一个csv，9334455, 0.2, 0.3, ...... 大概这个格式
	@Override
	protected void map(LongWritable key, Text value, Context context)
	  throws IOException, InterruptedException {
		
		//value是一行，包含csv文件的一列
		String s = value.toString();
		String[] ss = s.split(",");
		String time = ss[0];
		double[] values = new double[ss.length - 1];
		//不包含时间
		for (int i = 1; i < ss.length; i++){
			values[i - 1] = Double.parseDouble(ss[i]);
		}
		
		//TODO 转换成List, 这是历史问题，后续不需要list，直接使用数组就好了
		List<Double> valueList = new ArrayList<>();
		for (int i = 0; i < values.length; i++){
			valueList.add(values[i]);
		}

		//踢出奇异点
		valueList = Common.removeBadPoints(valueList);
		
		double MAX = 0;
		double MIN = 0;
		double RMS = 0;
		//更新均值
		double AVERAGE = Common.getListAverage(valueList);
		//MP.println("average is " + AVERAGE);
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
		double CLF = 0;
		if (XR != 0){
			CLF = MAX / XR;
		}
		//峭度因子
		double BEIDA = 0;
		for (int i = 0; i < valueList.size(); i++){
			BEIDA += Math.pow(valueList.get(i), 4);
		}
		BEIDA = BEIDA / valueList.size();
		double KV = BEIDA / Math.pow(RMS, 4);
		
		//NOTE 现在的顺序决定了第二个job的获取数据顺序，不能随便更改!
		String join = MyString.join(",", String.valueOf(RMS), String.valueOf(XPP), String.valueOf(SF), 
				String.valueOf(CF), String.valueOf(IF), String.valueOf(CLF), String.valueOf(KV));
		resultArrayString.set(join);
		outKey.set(time);
		//把时间--特征值传出
	    context.write(outKey, resultArrayString);
	}

}
