package rms;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.DoubleArrayWritable;
import utils.MP;

public class PcaMapper extends Mapper<LongWritable, Text, Text, DoubleArrayWritable>  {
	
	//传二维数组给reducer
	private DoubleArrayWritable resultValue = null;
	//第二个job的mapper输出的key保持一致就可以，可以任意。
	private static Text resultKey = new Text("second");
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
	  throws IOException, InterruptedException {
		
		// a. 分隔一行
		String s = value.toString();
		String[] values = s.split(",");
		for (int i = 0; i < values.length; i++){
			values[i] = values[i].trim();
		}
		DoubleWritable[] arr = new DoubleWritable[values.length];
		for (int i = 0; i < values.length; i++){
			//initialize arr, very important!
			arr[i] = new DoubleWritable(0);
		}
		// b.转换成DoubleWritable数组
		for (int i = 0; i < values.length; i++){
			//第一列是时间，也保留，时间是int类型，但是double也可以解析
			double temp = Double.valueOf(values[i]);
			arr[i].set(temp);
		}
		//not static
		resultValue = new DoubleArrayWritable(arr);
		MP.logln("result value is:", false);
		for (int i = 0; i < arr.length; i++){
			MP.log(arr[i] + " ", false);
		}
		context.write(resultKey, resultValue);
	}
	
}
