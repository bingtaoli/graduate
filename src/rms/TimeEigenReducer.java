package rms;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//把mapper传来的time--特征值写入hdfs
public class TimeEigenReducer extends Reducer<Text, Text, Text, Text>  {

	public void reduce(Text key, Iterable<Text> values,  Context context) 
			throws IOException, InterruptedException {
		
		//values一般数量为1
		Text outValue = new Text();
		for (Text val : values) {
			outValue = val;
			break;
		}
		context.write(key, outValue);
	}
}
