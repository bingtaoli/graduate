package rms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import utils.DoubleArrayWritable;
import utils.MyTimer;

/**
 * 耗时记录
 * 100个文件: cost time: 6.348
 * 600个文件: cost time: 8.857
 * 2800个文件: cost time: 27.216
 * 
 * NOTE:
 * job.setOutputKeyClass是针对map输出的结果设定
 * job.waitForCompletion(true)一定在设置完输出文件目录后
 * 
 * API:
 * job.setNumReduceTasks(0);
 * 
 */
public class Rms {
	
	public static void main(String[] args) throws Exception {
		MyTimer.setMark("all");
		
		/**
		 * arguments parse
		 */
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
	    	System.err.println("Usage: <in> <out>");
	    	System.exit(2);
	    }
	    
	    /**
	     * PCA first job 
	     */
//		Job job = Job.getInstance(conf, "pca first job");
//	    job.setJarByClass(Rms.class);
//	    job.setInputFormatClass(MyCombineFileInputFormat.class);
//	    job.setMapperClass(FirstMapper.class);
//	    job.setReducerClass(FirstReducer.class);
//	    job.setOutputKeyClass(Text.class);
//	    job.setOutputValueClass(DoubleWritable.class);
//	    FileInputFormat.setInputPaths(job, new Path(args[0]));
//	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
//	    job.waitForCompletion(true);
//        
//	    /**
//	     * PCA second job
//	     */
//        Job job2 = Job.getInstance(conf, "pca second job");
//	    job2.setJarByClass(Rms.class);
//	    job2.setInputFormatClass(MyCombineFileInputFormat.class);
//	    job2.setMapperClass(SecondMapper.class);
//	    job2.setReducerClass(SecondReducer.class);
//	    job2.setOutputKeyClass(Text.class);
//	    job2.setOutputValueClass(DoubleArrayWritable.class);
//        FileInputFormat.addInputPath(job2, new Path(args[1]));
//        FileOutputFormat.setOutputPath(job2, new Path(args[2]) );
//        job2.waitForCompletion(true);
        
        /**
         * EMD
         */
        Job emdJob = Job.getInstance(conf, "pca first job");
        emdJob.setJarByClass(Rms.class);
        emdJob.setInputFormatClass(MyCombineFileInputFormat.class);
        emdJob.setMapperClass(FirstMapper.class);
        emdJob.setReducerClass(EmdReducer.class);
        //emdJob.setNumReduceTasks(0);
        emdJob.setOutputKeyClass(Text.class);
        emdJob.setOutputValueClass(DoubleWritable.class);
	    FileInputFormat.setInputPaths(emdJob, new Path(args[0]));
	    FileOutputFormat.setOutputPath(emdJob, new Path(args[1]));
	    emdJob.waitForCompletion(true);
        
	    System.out.println("cost time: " + MyTimer.getCost("all") / 1000 + "s");
	    System.exit(1);
	}
}
