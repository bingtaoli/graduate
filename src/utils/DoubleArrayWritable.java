package utils;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

public class DoubleArrayWritable extends ArrayWritable {

	public DoubleArrayWritable() {
	    super(DoubleWritable.class);
	}
	 
	public DoubleArrayWritable(DoubleWritable[] ds) {
	    super(DoubleWritable.class, ds);
	}
	
}
