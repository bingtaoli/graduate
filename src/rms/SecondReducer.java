package rms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import algorithm.Normalization;
import algorithm.Pca;
import utils.DoubleArrayWritable;

public class SecondReducer extends Reducer<Text, DoubleArrayWritable, Text, Text> {
	
	public void reduce(Text key, Iterable<DoubleArrayWritable> values,  Context context) 
			throws IOException, InterruptedException {
		
		List<Double> RMSList = new ArrayList<>();
		List<Double> XPPList = new ArrayList<>();
		List<Double> SFList = new ArrayList<>();
		List<Double> CFList = new ArrayList<>();
		List<Double> IFList = new ArrayList<>();
		List<Double> CLFList = new ArrayList<>();
		List<Double> KVList = new ArrayList<>();
		//TODO 不用每个分别一个名称
		List<List<Double>> allList = new ArrayList<>();
		allList.add(RMSList);
		allList.add(XPPList);
		allList.add(SFList);
		allList.add(CFList);
		allList.add(IFList);
		allList.add(CLFList);
		allList.add(KVList);
		
		int length = 0;
		
		//转换成List
		List<DoubleWritable[]> valueList = new ArrayList<>();
		
		for (DoubleArrayWritable val : values) {
			//链表每一个元素都是数组
			DoubleWritable[] temp = (DoubleWritable[]) val.toArray();
			valueList.add(temp);
			length++;
		}
		
		for (int i = 0; i < valueList.size(); i++){
			DoubleWritable[] temp = valueList.get(i);
			for (int j = 0; j < allList.size(); j++){
				//第一个是时间，略过
				allList.get(j).add(temp[j+1].get());
			}
		}
		
		//归一化
		for (int i = 0; i < allList.size(); i++){
			allList.set(i, Normalization.normalizatioList(allList.get(i)));
		}
		//主成分分析
		double[][] array = new double[length][allList.size()];
		for (int i = 0; i < length; i++){
			for (int j = 0; j < allList.size(); j++){
				array[i][j] = allList.get(j).get(i);
			}
		}
		Pca.calculate(array);
	}
}
