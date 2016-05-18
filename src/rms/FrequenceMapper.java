package rms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.MP;
import utils.MyString;

public class FrequenceMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public static Text resultKey = new Text();
	public static Text resultValue = new Text();
	
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
		
		int originDataLength = valueList.size();
		double interval = 0.1 / 2560;
		double sum2ToN = 0;
		double sum1ToN = 0;
		for (int i = 1; i < originDataLength; i++){
			sum2ToN += Math.pow(valueList.get(i), 2);
		}
		sum1ToN = sum2ToN + valueList.get(0);
		double MSF = sum2ToN / (4 * Math.PI * Math.PI * sum1ToN);
		double sumOfSpeed = 0;  //斜率和
		double formalData = valueList.get(0);
		for (int i = 1; i < originDataLength; i++){
			double speed = (valueList.get(i) - formalData) / interval;
			sumOfSpeed += speed;
		}
		double FC = 0;
		sum2ToN = 0;
		for (int i = 1; i < originDataLength; i++){
			sum2ToN += sumOfSpeed * valueList.get(i);
		}
		FC = sum2ToN / (2 * Math.PI * sum1ToN);
		double VF = MSF - Math.pow(FC, 2);
		double RMSF = MSF > 0 ? Math.sqrt(MSF) : 0;
		double RVF = VF > 0 ? Math.sqrt(VF) : 0;
		
		//print for testing
		String result =  MyString.join(",", MSF+"", FC+"", VF+"", RMSF+"", RVF+"");
		MP.logln("result: " + result);
		
		resultKey.set(time);
		resultValue.set(result);
		context.write(resultKey, resultValue);
	}

}
