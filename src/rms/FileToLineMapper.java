package rms;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.MP;

/**
 * 把一个csv文件变成一行
 * @author matthew
 */
public class FileToLineMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	/**
	 * 多文件会初始化很多次map类
	 * 单个文件会吗？
	 */
	public static double costMapperTime = 0;
	public static DoubleWritable resultValue = new DoubleWritable();
	public static Text resultKey = new Text();
	public static int number = 0;
	public int index = -1;
	
	public FileToLineMapper() {
		MP.logln("create a mapper>>>>>>>> NO: " + number);
		index = number;
		number++;
	}
	
	protected void finalize(){
		MP.logln(">>>>>>>>>end of mapper NO:" + index);
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
	  throws IOException, InterruptedException {
		//把value分隔
		String s = value.toString();
		String[] values = s.split(",");
		String time;
		//时间，垂直信号，水平信号
		if (values.length >= 5){
			//增加一个逗号，方便后面split
			time =  String.format("%02d%02d%02d,", Integer.valueOf(values[0]) ,
					Integer.valueOf(values[1]), Integer.valueOf(values[2]));
			resultKey.set(time);
			//2. 当前行的倒数第二列
			resultValue.set(Double.valueOf(values[4]));
			context.write(resultKey, resultValue);
		}
	}
}