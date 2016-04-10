package rms;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondJobMapper extends Mapper<LongWritable, Text, Text, ArrayWritable>  {
	
	//传二维数组给reducer
	private static ArrayWritable resultValue = new ArrayWritable(DoubleWritable.class);
	//第二个job的mapper输出的key保持一致就可以，可以任意。
	private static Text resultKey = new Text("second");
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
	  throws IOException, InterruptedException {
		// a. 分隔一行
		String s = value.toString();
		String[] values = s.split(",");
		DoubleWritable[] arr = new DoubleWritable[values.length - 1];
		// b.转换成DoubleWritable数组
		for (int i = 0; i < values.length; i++){
			//第一列是时间，也保留，时间是int类型，但是double也可以解析
			arr[i].set(Double.parseDouble(values[i]));
		}
		resultValue.set(arr);
		context.write(resultKey, resultValue);
	}
	
}
