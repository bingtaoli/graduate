package rms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.Rengine;

import algorithm.Common;
import utils.DoubleArrayWritable;
import utils.MP;
import utils.MyR;
import utils.MyString;

public class EmdMapper extends Mapper<LongWritable, Text, Text, DoubleArrayWritable> {
	
	public DoubleArrayWritable resultArrayValue = new DoubleArrayWritable();
	public Text resultKey = new Text();
	
	//key是行号，value是一行，代表一个csv，9334455, 0.2, 0.3, ...... 大概这个格式
	@Override
	protected void map(LongWritable key, Text value, Context context)
	  throws IOException, InterruptedException {
		
		String s = value.toString();
		String[] values = s.split(",");
		for (int i = 0; i < values.length; i++){
			values[i] = values[i].trim();
		}
		String time = values[0];
		DoubleWritable[] arr = new DoubleWritable[values.length - 1];
		for (int i = 0; i < values.length - 1; i++){
			//initialize arr, very important!
			arr[i] = new DoubleWritable(0);
		}
		// b.转换成DoubleWritable数组
		for (int i = 0; i < values.length - 1; i++){
			double temp = Double.valueOf(values[i+1]);
			arr[i].set(temp);
		}
		
		resultArrayValue.set(arr);
		resultKey.set(time);
		context.write(resultKey, resultArrayValue);
	}
	
}
