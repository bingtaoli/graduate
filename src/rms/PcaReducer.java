package rms;

import java.io.IOException;
import java.sql.SQLException;
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
import org.ujmp.core.DenseMatrix;
import org.ujmp.core.Matrix;

import DB.Mysql;
import algorithm.Normalization;
import algorithm.Pca;
import utils.ArrayToStr;
import utils.DoubleArrayWritable;
import utils.MP;
import utils.MyR;

/**
 * @author matthew
 * 第二个job输出是pca后得到的矩阵
 * eigen value: 特征值
 */
public class PcaReducer extends Reducer<Text, DoubleArrayWritable, Text, Text> {
	
	public static int EIGENVALUECOUNT = 7;  //特征值个数
	public static double PCAPRECISE = 0.9;
	
	private static Text resultKey = new Text("");
	private static Text resultValue = new Text("");
	
	public void reduce(Text key, Iterable<DoubleArrayWritable> values,  Context context) 
			throws IOException, InterruptedException {
		
		Rengine re = MyR.getREngine();
		
        //每个元素都是一个数组，每个数组有7个特征值
		List<double[]> valueList = new ArrayList<>();
		
		for (DoubleArrayWritable val : values) {
			DoubleWritable[] temp = (DoubleWritable[]) val.toArray();
			int length = temp.length;
			double[] eigenValueArray = new double[length-1];
			//省略第一列的时间
			for (int i = 0; i < length - 1; i++){
				eigenValueArray[i] = temp[i+1].get();
			}
			valueList.add(eigenValueArray);
		}
		
		/**
	     * using Rlang for pca
	     */
		//la是特征值二维矩阵
		long[] la = new long[valueList.size()];
		for (int i = 0; i < valueList.size(); i++){
			long temp = re.rniPutDoubleArray(valueList.get(i));
			la[i] = temp;
		}
		//把特征值矩阵赋给re中的变量
		long xp5 = re.rniPutVector(la);
        re.rniAssign("eigen", xp5, 0);
        re.eval("eigen <- data.frame(eigen)");
        MP.log("eigen's length is: ");
        REXP eigen = re.eval("length(eigen)");
        MP.logln(eigen);
        REXP pca;
        REXP pcaPredict;
        pcaPredict = re.eval("predict(prcomp(eigen))");
        /**
         * pca predict是一个一维度数组，其实这个一维数组包含了几维的数据
         * predict是特征向量矩阵，为 7*7矩阵
         * 比如可以只取前2列，然后7*2矩阵和原来的矩阵相乘得到最后结果
         */
        double[] pcaResultArray = pcaPredict.asDoubleArray();
        //贡献率
        MP.logln("eigen pca contribution is: ");
        MP.logln(pca=re.eval("summary(prcomp(eigen))$importance[3,]"));
        double[] pcaContributionArray = pca.asDoubleArray();
	   
        re.end();
	    MP.println("r engine end");
	    MP.println("");
	    
	    double contribution = 0;
	    int count = 0;
	    for (int i = 0; i < pcaContributionArray.length; i++){
	    	contribution += pcaContributionArray[i];
	    	//贡献率达到精确度要求
	    	if (contribution > PCAPRECISE){
	    		count = i;
	    		break;
	    	}
	    }
	    
	    //取前count列的特征向量，得到 7 * count的矩阵 
	    double[][] pcaResultTwoDimension = new double[EIGENVALUECOUNT][count+1];
	    for (int i = 0; i < EIGENVALUECOUNT; i++){
	    	for (int j = 0; j < count + 1; j++){
	    		if (i * EIGENVALUECOUNT + j < pcaResultArray.length){
	    			pcaResultTwoDimension[i][j] = pcaResultArray[i*7+j];
	    		}
	    	}
	    }
	    //打印前index列的pca predict特征值结果
	    //现在的到的结果为特征矩阵的前count列
	    MP.println("pca predict result is: ");
	    for (int i = 0; i < EIGENVALUECOUNT; i++){
	    	for (int j = 0; j < count + 1; j++){
	    		MP.print(pcaResultTwoDimension[i][j] + "  ");
	    	}
	    	MP.println();
	    }
	    
	    //原始矩阵和特征向量矩阵相乘
	    Matrix finalData = DenseMatrix.Factory.zeros(valueList.size(), count + 1);
	    double[][] originData = new double[valueList.size()][EIGENVALUECOUNT];
	    for (int i = 0; i < valueList.size(); i++){
	    	for (int j = 0; j < EIGENVALUECOUNT; j++){
	    		originData[i][j] = valueList.get(i)[j];
	    	}
	    }
	    Matrix originMatrix = Matrix.Factory.linkToArray(originData);
	    Matrix eigenMatrix = Matrix.Factory.linkToArray(pcaResultTwoDimension);
	    finalData = originMatrix.mtimes(eigenMatrix);
	    
	    MP.println("");
	    MP.println("final pca data is:");
		MP.println(finalData);
		
		double[][] result = finalData.toDoubleArray();
		/**
		 * 写进数据库中
		 */
//		String encodedStr = ArrayToStr.encodeTwoDimensionArray(result); //数组encode成str
//	    Mysql db = new Mysql("test", "root");
//	    db.initConnection();
//	    String sql = "insert into hadoop(pcaResult) " + "values ('" + encodedStr + "');"; //NO MISSING ' '
//	    try {
//	    	MP.logln("execute sql: " + sql, false);
//			db.getStmt().executeUpdate(sql);
//			MP.logln("insert to db  success success success!!", false);
//		} catch (SQLException e) {
//			MP.println("insert to db failure failure failure :(");
//			e.printStackTrace();
//		} finally {
//			db.close();
//		}
//	    
	    //把pca结果写入csv文件，便于分析走势
	    Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path outputdir = new Path("/output/pcaresult.txt");
		FSDataOutputStream out = hdfs.create(outputdir);
		//out.writeBytes("pca result: \n");
		String s;
		for (int i = 0; i < result.length; i++){
			s = "";
			for (int j = 0; j < result[i].length - 1; j++){
				s += result[i][j] + ",";
			}
			//最后一列不加逗号
			s += result[i][result[i].length - 1];
			out.writeBytes(s + "\n");
		}
	    
	    // TODO 把pca的结果作为reducer的输出
	    
	}
	
	
	public void __reduce(Text key, Iterable<DoubleArrayWritable> values,  Context context) 
			throws IOException, InterruptedException {
		
		List<Double> RMSList = new ArrayList<>();
		List<Double> XPPList = new ArrayList<>();
		List<Double> SFList = new ArrayList<>();
		List<Double> CFList = new ArrayList<>();
		List<Double> IFList = new ArrayList<>();
		List<Double> CLFList = new ArrayList<>();
		List<Double> KVList = new ArrayList<>();
		//TODO 不用每个分别一个名称
		List<List<Double>> allList = new ArrayList<>();
		allList.add(RMSList);
		allList.add(XPPList);
		allList.add(SFList);
		allList.add(CFList);
		allList.add(IFList);
		allList.add(CLFList);
		allList.add(KVList);
		
		int length = 0;
		
		//转换成List
		List<DoubleWritable[]> valueList = new ArrayList<>();
		
		for (DoubleArrayWritable val : values) {
			//链表每一个元素都是数组，数组有8个元素，第一个是时间
			DoubleWritable[] temp = (DoubleWritable[]) val.toArray();
			valueList.add(temp);
			length++;
		}
		
		for (int i = 0; i < valueList.size(); i++){
			DoubleWritable[] temp = valueList.get(i);
			for (int j = 0; j < allList.size(); j++){
				//第一个是时间，略过
				allList.get(j).add(temp[j+1].get());
			}
		}
		
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path normalizationOutputDir = new Path("/output/normalization.txt");
		FSDataOutputStream out = hdfs.create(normalizationOutputDir);
		out.writeBytes("normalization result: \n");
		//归一化
		for (int i = 0; i < allList.size(); i++){
			allList.set(i, Normalization.normalizatioList(allList.get(i)));
		}
		// 归一化结果输出到HDFS中
		String s;
		for (int i = 0; i < valueList.size(); i++){
			s = " ";
			for (int j = 0; j < allList.size(); j++){
				s += allList.get(j).get(i) + " ";
			}
			out.writeBytes(s + "\n");
		}
		
		//主成分分析
		double[][] array = new double[length][allList.size()];
		for (int i = 0; i < length; i++){
			for (int j = 0; j < allList.size(); j++){
				array[i][j] = allList.get(j).get(i);
			}
		}
	    double result[][] = Pca.calculate(array);
	    
	    //数组encode成str
	    String encodedStr = ArrayToStr.encodeTwoDimensionArray(result);
	    
	    //写进数据库中
	    Mysql db = new Mysql("test", "root");
	    db.initConnection();
	    //do not missing ' ' to wrap encodeStr
	    String sql = "insert into hadoop(pcaResult) " + "values ('" + encodedStr + "');";
	    
	    try {
	    	MP.println("execute sql: " + sql);
			db.getStmt().executeUpdate(sql);
			MP.println("insert to db  success success success!!");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			MP.println("insert to db failure failure failure :(");
			e.printStackTrace();
		}
	    
	    for (int i = 0; i < result.length; i++){
	    	String temp = "";
	    	for (int j = 0; j < result[i].length; j++){
	    		temp += " " + String.valueOf(result[i][j]);
	    	}
	    	resultValue.set(temp);
	    	context.write(resultKey, resultValue);
	    }
	}
}
