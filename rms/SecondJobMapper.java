package rms;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondJobMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>  {
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
	  throws IOException, InterruptedException {
		System.out.println(value);
	}
	
}
