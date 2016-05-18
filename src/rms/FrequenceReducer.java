package rms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.MP;
import utils.MyString;

public class FrequenceReducer extends Reducer<Text, DoubleWritable, Text, Text> {
	
	public void reduce(Text key, Iterable<DoubleWritable> values,  Context context) 
			throws IOException, InterruptedException {
		//转换成List
		List<Double> valueList = new ArrayList<>();
		for (DoubleWritable val : values) {
			valueList.add(val.get());
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
		MP.logln("result: " + MyString.join(",", MSF+"", FC+"", VF+"", RMSF+"", RVF+""));
	}
}
