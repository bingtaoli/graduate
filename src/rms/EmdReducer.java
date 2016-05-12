package rms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import algorithm.Common;

/**
 * @author matthew
 * 得到`<time, iterator<double>>`，处理iterator，得到倒数第二列，进行`emd`处理。得到imf，再求边际谱。
 *
 */
public class EmdReducer extends Reducer<Text, DoubleWritable, Text, Text>  {
	
	//输入是时间--每一行倒数第二列的double值
		public void reduce(Text key, Iterable<DoubleWritable> values,  Context context) 
				throws IOException, InterruptedException {
			
			//转换成List
			List<Double> valueList = new ArrayList<>();
			for (DoubleWritable val : values) {
				valueList.add(val.get());
			}
			double tempAverage = Common.getListAverage(valueList);
			double sigma = Common.getListStandardDevition(valueList, tempAverage);
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
			/**
			 * TODO 求emd
			 */
			
		}

}
