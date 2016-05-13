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
import utils.MP;
import utils.MyR;
import utils.MyString;

/**
 * @author matthew
 * 得到`<time, iterator<double>>`，处理iterator，得到倒数第二列，进行`emd`处理。得到imf，再求边际谱。
 *
 */
public class EmdReducer extends Reducer<Text, DoubleWritable, Text, Text>  {
	
	//输入是时间--每一行倒数第二列的double值
	public void reduce(Text key, Iterable<DoubleWritable> values,  Context context) 
			throws IOException, InterruptedException {
		
		//转换成List
		List<Double> valueList = new ArrayList<>();
		for (DoubleWritable val : values) {
			valueList.add(val.get());
		}
		double tempAverage = Common.getListAverage(valueList);
		double sigma = Common.getListStandardDevition(valueList, tempAverage);
		//踢除奇异点
		//a.第一行特殊处理
		if ( Math.abs(valueList.get(0) - tempAverage)  >= 3*sigma ){
			valueList.set(0, tempAverage);
		}
		//b.其他行数
		for (int i = 1; i < valueList.size(); i++){
			if ( Math.abs(valueList.get(i) - tempAverage)  >= 3*sigma ){
				valueList.set(i, valueList.get(i-1));
			}
		}
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

		//load emd library
		re.eval("library(EMD)");
		
		testEMD(re, false);
		
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
		
		//load signal library for interp1
		re.eval("library(signal)");
		
		double[] hir = new double[imfLength - 1];
		double[] hor = new double[imfLength - 1];
		double[] hbr = new double[imfLength - 1];
		// 1 ~ imfLength
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		Path outputdir = new Path("/output/hilbertresult.txt");
		FSDataOutputStream hilbertResultOut = hdfs.create(outputdir);
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
			
			String s;
			for (int i = 0; i < ampArray.length; i++){
				s = "" + ampArray[i] + ", " + freqArray[i];
				hilbertResultOut.writeBytes(s + "\n");
			}
			
			re.eval("bigger <- floor(400/max(insfreq, na.rm=TRUE))"); //放大倍数
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
			
			
			MP.logln("hir is: " + re.eval("hir"));
			MP.logln("hor is: " + re.eval("hor"));
			MP.logln("hbr is: " + re.eval("hbr"));
			
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
		Path outputdir2 = new Path("/output/emdresult.txt");
		FSDataOutputStream out = hdfs.create(outputdir2);
		String s;
		s = MyString.join(",", key.toString(), hirMax+" ",  horMax+" ",  hbrMax+" ");
		out.writeBytes(s + "\n");
		
		re.end();
	    MP.println("r engine end");
	    MP.println("");
	}
	
	private void testEMD(Rengine re, boolean test){
		if (test){
			//test emd
			re.eval("ndata <- 3000");
			re.eval("tt22 <- seq(0, 9, length=ndata)");
			MP.logln("ndata is: " +  re.eval("ndata"));
			re.eval("xt22 <- sin(pi * tt22) + sin(2* pi * tt22) + sin(6 * pi * tt22) ");
			re.eval("try22 <- emd(xt22, tt22, boundary=\"none\")");
			MP.logln("try22 is: " +  re.eval("try22"));
			MP.logln("try22 imfs length is: " +  re.eval("try22$nimf"));
		}
		return;
	}
	
}
