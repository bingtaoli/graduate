package rms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FileToLineReducer extends Reducer<Text, DoubleWritable, Text, Text>  {
	
	private Text resultArrayString = new Text();
	
	/**
	 * 变成文件的一行
	 */
	//key是时间， values是每一行倒数第二列的double值
	public void reduce(Text key, Iterable<DoubleWritable> values,  Context context) 
			throws IOException, InterruptedException {
		
		//转换成List
		List<Double> valueList = new ArrayList<>();
		for (DoubleWritable val : values) {
			valueList.add(val.get());
		}
		
		String s = "";
		int i;
		for (i = 0; i < valueList.size() - 1; i++){
			s += valueList.get(i) + ",";
		}
		s += valueList.get(i);  //最后一个
		//把倒数第二行变成了一列输出
		resultArrayString.set(s);
		context.write(key, resultArrayString);
	}
}
