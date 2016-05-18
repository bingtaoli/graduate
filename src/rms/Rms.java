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
		//一个input， 5个输出
	    if (otherArgs.length < 6) {
	    	System.err.println("Usage: <in> <out>");
	    	System.exit(6);
	    }
	    
	    /**
	     * 设想，建立一个新的job，把2080个文件转换成一个文件的2080行
	     * 然后后续的job都可以以mapper来处理该文件，并且是并行的
	     * FileToLine job
	     */
		Job fileToLineJob = Job.getInstance(conf, "file to line job");
	    fileToLineJob.setJarByClass(Rms.class);
	    fileToLineJob.setInputFormatClass(MyCombineFileInputFormat.class);
	    fileToLineJob.setMapperClass(FileToLineMapper.class);
	    fileToLineJob.setReducerClass(FileToLineReducer.class);
	    fileToLineJob.setOutputKeyClass(Text.class);
	    fileToLineJob.setOutputValueClass(DoubleWritable.class);
	    FileInputFormat.setInputPaths(fileToLineJob, new Path(args[0]));
	    FileOutputFormat.setOutputPath(fileToLineJob, new Path(args[1]));
	    fileToLineJob.waitForCompletion(true);
	    
	    /**
	     * Time Eigne Job
	     * 求时间特征量
	     */
//	    Job timeEigenJob = Job.getInstance(conf, "time eigen job");
//	    timeEigenJob.setJarByClass(Rms.class);
//	    timeEigenJob.setInputFormatClass(MyCombineFileInputFormat.class);
//	    timeEigenJob.setMapperClass(TimeEigenMapper.class);
//	    timeEigenJob.setReducerClass(TimeEigenReducer.class);
//	    timeEigenJob.setOutputKeyClass(Text.class);
//	    timeEigenJob.setOutputValueClass(Text.class);
//	    FileInputFormat.setInputPaths(timeEigenJob, new Path(args[1]));
//	    FileOutputFormat.setOutputPath(timeEigenJob, new Path(args[2]));
//	    timeEigenJob.waitForCompletion(true);
	    
	    /**
	     * PCA job
	     * 处理第一个job得到的特征量
	     */
//        Job pcaJob = Job.getInstance(conf, "pca job");
//	    pcaJob.setJarByClass(Rms.class);
//	    pcaJob.setInputFormatClass(MyCombineFileInputFormat.class);
//	    pcaJob.setMapperClass(PcaMapper.class);
//	    pcaJob.setReducerClass(PcaReducer.class);
//	    pcaJob.setOutputKeyClass(Text.class);
//	    pcaJob.setOutputValueClass(DoubleArrayWritable.class);
//        FileInputFormat.addInputPath(pcaJob, new Path(args[2]));  //Time Eigne Job的输出，即时间特征值
//        FileOutputFormat.setOutputPath(pcaJob, new Path(args[3]) );
//        pcaJob.waitForCompletion(true);
        
        /**
         * EMD Job
         */
//        Job emdJob = Job.getInstance(conf, "emd job");
//        emdJob.setJarByClass(Rms.class);
//        emdJob.setInputFormatClass(MyCombineFileInputFormat.class);
//        emdJob.setMapperClass(EmdMapper.class);
//        emdJob.setNumReduceTasks(0); //无reduce操作
//        emdJob.setOutputKeyClass(Text.class);
//        emdJob.setOutputValueClass(Text.class);
//	    FileInputFormat.setInputPaths(emdJob, new Path(args[1]));  //file to line job的输出
//	    FileOutputFormat.setOutputPath(emdJob, new Path(args[4]));
//	    emdJob.waitForCompletion(true);
//	    
	    /**
	     * Frequence Job
	     */
	    Job frequenceJob = Job.getInstance(conf, "emd job");
        frequenceJob.setJarByClass(Rms.class);
        frequenceJob.setInputFormatClass(MyCombineFileInputFormat.class);
        frequenceJob.setMapperClass(FrequenceMapper.class);
        frequenceJob.setNumReduceTasks(0);
        frequenceJob.setOutputKeyClass(Text.class);
        frequenceJob.setOutputValueClass(Text.class);
	    FileInputFormat.setInputPaths(frequenceJob, new Path(args[1])); //file to line job的输出
	    FileOutputFormat.setOutputPath(frequenceJob, new Path(args[5]));
	    frequenceJob.waitForCompletion(true);
        
	    System.out.println("cost time: " + MyTimer.getCost("all") / 1000 + "s");
	    System.exit(1);
	}
}
