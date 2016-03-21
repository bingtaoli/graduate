package rms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 耗时记录
 * 100个文件: cost time: 6.348
 * 600个文件: cost time: 8.857
 * 2800个文件: cost time: 27.216
 */

public class Rms extends Configured implements Tool {	
	@Override
	public int run(String[] args) throws Exception {
	    Configuration conf = new Configuration();

	    @SuppressWarnings("deprecation")
		Job job = new Job(conf);
	    job.setJobName("CSVTest");
	    job.setJarByClass(Rms.class);
	    
	    job.setInputFormatClass(CSVCombineFileInputFormat.class);
	    job.setMapperClass(CSVCombineFileMapper.class);
	    job.setReducerClass(CSVReducer.class);
	    
	    //该设置是针对map的输出？醉了.
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    
	    //job.setNumReduceTasks(0);

	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	    	System.err.println("Usage: csv <in> <out>");
	    	System.exit(2);
	    }
	    
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
		MyTimer.setMark("all");
	    int ret = ToolRunner.run(new Rms(), args);
	    System.out.println("cost time: " + MyTimer.getCost("all") / 1000);
	    System.exit(ret);
	}
}
