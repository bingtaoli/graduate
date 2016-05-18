package rms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.Rengine;

import algorithm.Common;
import utils.DoubleArrayWritable;
import utils.MP;
import utils.MyR;
import utils.MyString;

public class EmdReducer extends Reducer<Text, DoubleArrayWritable, Text, Text> {
	
	public static int currentNumber = 1;
	public static FSDataOutputStream hilbertResultOut = null;
	public static FSDataOutputStream frequenceAnalyseResultOut = null;
	public static Text resultValue = new Text();
	
	public EmdReducer() {
		// TODO Auto-generated constructor stub
		MP.logln("emd reducer number " + currentNumber);
		currentNumber++;
	}
	
	static {
		Configuration conf = new Configuration();
		FileSystem hdfs;
		try {
			hdfs = FileSystem.get(conf);
			Path outputdir2 = new Path("/output/frequenceAnalyseResult.txt");
			frequenceAnalyseResultOut = hdfs.create(outputdir2);
			Path outputdir = new Path("/output/hilbertresult.txt");
			hilbertResultOut = hdfs.create(outputdir);
			MP.logln("initialize csv output file finished");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void reduce(Text key, Iterable<DoubleArrayWritable> arrayValues,  Context context) 
			throws IOException, InterruptedException {
		
		DoubleArrayWritable doubleArrayWritable = null;
		for (DoubleArrayWritable val : arrayValues){
			doubleArrayWritable = val;
			break;
		}
		if (doubleArrayWritable == null){
			MP.logln("double array writable null");
			return;
		}
		DoubleWritable[] temp = (DoubleWritable[]) doubleArrayWritable.toArray();
		double[] values = new double[temp.length];
		for (int i = 0; i < values.length; i++){
			values[i] = temp[i].get();
		}

		//TODO 转换成List, 这是历史问题，后续不需要list，直接使用数组就好了
		List<Double> valueList = new ArrayList<>();
		for (int i = 0; i < values.length; i++){
			valueList.add(values[i]);
		}
		
		//踢出奇异点
		valueList = Common.removeBadPoints(valueList);
		
		int originDataLength = valueList.size();
		/**
		 * TODO R语言求emd
		 */
		Rengine re = MyR.getREngine();
		
		double[] originData = new double[valueList.size()];
		for (int i = 0; i < valueList.size(); i++){
			originData[i] = valueList.get(i);
		}
		long rOriginVector = re.rniPutDoubleArray(originData);
		
		re.rniAssign("originVector", rOriginVector, 0);
		//NOTE 时间序列暂时为 1:length
		re.eval("tt <- 1 : " + valueList.size());
		MP.logln("value length is: " + valueList.size(), false);
		MP.logln("originVector is: " +  re.eval("originVector"), false);
		MP.logln("tt is: " +  re.eval("tt"), false);
		
		//对origin数据求emd
		re.eval("imfs <- emd(originVector, tt, boundary=\"none\")");
		
		MP.logln("imfs is: " +  re.eval("imfs"), false);
		REXP imfLengthRexp = re.eval("imfs$nimf");
		
		MP.logln("imf length is: " +  imfLengthRexp);
		double imfLengthDouble = imfLengthRexp.asDouble();
		MP.logln("imf length as Double is: " +  imfLengthDouble);
		int imfLength = (int)imfLengthDouble;
		
		if (imfLength <= 1){
			MP.logln("imf length less than 1, return directly");
			return;
		}
		
		double[] hir = new double[imfLength - 1];
		double[] hor = new double[imfLength - 1];
		double[] hbr = new double[imfLength - 1];
		
		for (int index = 1; index < imfLength; index++){
			re.eval("x <- matrix(imfs$imf[," + index +  "], " + valueList.size()  + ", 1)");
			//希尔伯特变换
			re.eval("test2 <- hilbertspec(x)");
			
			MP.logln("after hilbertspec", false);
			
			re.eval("amplitude <- test2$amplitude[,1]");
			re.eval("insfreq <- test2$instantfreq[,1]");
			
			REXP amp = re.eval("amplitude");
			REXP freq = re.eval("insfreq");
			
			MP.logln("amplitude is: ", false);
			MP.logln(re.eval("amplitude"), false);
			
			//把amplitude和instantfreq输出到csv
			double[] ampArray = amp.asDoubleArray();
			double[] freqArray = freq.asDoubleArray();
			
			if (ampArray.length != freqArray.length){
				MP.logln("errors happen, amp array length does not equal freq array length");
				return;
			}
			
			// put hilbert result to hdfs csv file for analysing
			String hilbertResult;
			for (int i = 0; i < ampArray.length; i++){
				hilbertResult = "" + ampArray[i] + ", " + freqArray[i];
				hilbertResultOut.writeBytes(hilbertResult + "\n");
			}
			
			//放大倍数：把频率从0~1的归一化结果放大到0~400
			re.eval("bigger <- floor(400/max(insfreq, na.rm=TRUE))"); 
			re.eval("dataLength <- " + originDataLength);
			re.eval("for (i in 1:dataLength) {"
					+ " if (is.na(insfreq[i])) {insfreq[i] = 0} "
					+ "insfreq[i] = floor(insfreq[i] * bigger)"
					+ "}");
			re.eval("toimageResult <- matrix(data=0, nrow=400, ncol=originDataLength)");
			re.eval("for (i in 1:originDataLength){"
					+ " freq <- insfreq[i]; "
					+ "toimageResult[freq, i] = amplitude[i]"
					+ "}");
			re.eval("bjp <- array(1:400)");
			re.eval("for (i in 1:400){ "
					+ "sum <- sum(toimageResult[i,], na.rm=TRUE); "
					+ "bjp[i] = sum"
					+ "}");
			re.eval("xx <- 1:400");
			re.eval("hir <- interp1(xx, bjp, 221)");
			re.eval("hor <- interp1(xx, bjp, 168)");
			re.eval("hbr <- interp1(xx, bjp, 215.48)");
			
			
			MP.logln("hir is: " + re.eval("hir"), 0);
			MP.logln("hor is: " + re.eval("hor"), 0);
			MP.logln("hbr is: " + re.eval("hbr"), 0);
			
			MP.logln("after interp1", false);
			
			double result1 = re.eval("hir").asDouble();
			double result2 = re.eval("hor").asDouble();
			double result3 = re.eval("hbr").asDouble();
			int hindex = index - 1; //hir索引为index-1
			hir[hindex] = result1;
			hor[hindex] = result2;
			hbr[hindex] = result3;
			MP.logln("" + hir[hindex] + " " + hor[hindex] + " " + hbr[hindex], false);
		}
		
		double hirMax = Common.maxOfArray(hir);
		double horMax = Common.maxOfArray(hor);
		double hbrMax = Common.maxOfArray(hbr);
		
		MP.logln("hirMax is: " + hirMax, false);
		
		//把结果写入CSV
		String result;
		result = MyString.join(",", key.toString(), hirMax+" ",  horMax+" ",  hbrMax+" ");
		MP.logln("write into csv: " + result + "\n");
		frequenceAnalyseResultOut.writeBytes(result + "\n");
		
		context.write(key, resultValue);
	}

}
