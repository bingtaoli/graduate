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

/**
 * 第二个job输出是pca后得到的矩阵
 * @author matthew
 * 
 */
public class SecondReducer extends Reducer<Text, DoubleArrayWritable, Text, Text> {
	
	public static int EIGENVALUECOUNT = 7;  //特征值个数
	
	private static Text resultKey = new Text("");
	private static Text resultValue = new Text("");
	
	private Rengine getREngine(){
		/**
		 * R engine test
		 */
		// just making sure we have the right version of everything
		if (!Rengine.versionCheck()) {
		    System.err.println("** Version mismatch - Java files don't match library version.");
		    System.exit(1);
		}
        System.out.println("Creating Rengine (with arguments)");
		// 1) we pass the arguments from the command line
		// 2) we won't use the main loop at first, we'll start it later
		//    (that's the "false" as second argument)
		// 3) the callbacks are implemented by the TextConsole class above
		Rengine re=new Rengine(new String[] { "--vanilla" }, false, null);
        System.out.println("Rengine created, waiting for R");
		// the engine creates R is a new thread, so we should wait until it's ready
        if (!re.waitForR()) {
            System.out.println("Cannot load R");
            return null;
        }
        return re;
	}
	
	public void reduce(Text key, Iterable<DoubleArrayWritable> values,  Context context) 
			throws IOException, InterruptedException {
		
		Rengine re = getREngine();
		
        /**
	     * using Rlang for pca
	     */
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
		
		long[] la = new long[valueList.size()];
		for (int i = 0; i < valueList.size(); i++){
			long temp = re.rniPutDoubleArray(valueList.get(i));
			la[i] = temp;
		}
		
		long xp5 = re.rniPutVector(la);
		
        re.rniAssign("b", xp5, 0);
        re.eval("b <- data.frame(b)");
        
        System.out.println("start of b test >>>>>>>>>>>>>>>>");
        System.out.print("b's length is >>>>>>>>>>>>>>>>  ");
        REXP b = re.eval("length(b)");
        System.out.println(b);
        System.out.println("b pca is >>>>>>>>>>>>>>>>");
        REXP pca;
        REXP pcaPredict;
        System.out.println("b pca predict is >>>>>>>>>>>>>>>>");
        pcaPredict = re.eval("predict(prcomp(b))");
        /**
         * pca predict是一个一维度数组
         * predict是特征向量矩阵，为 7*7矩阵
         * 比如可以只取前2列，然后7*2矩阵和原来的矩阵相乘得到最后结果
         */
        double[] pcaResultArray = pcaPredict.asDoubleArray();
        
        //贡献率
        System.out.println("b pca contribution is >>>>>>>>>>>>>>>>");
        System.out.println(pca=re.eval("summary(prcomp(b))$importance[3,]"));
        double[] pcaContributionArray = pca.asDoubleArray();
        System.out.println("");
        System.out.println("end of b test >>>>>>>>>>>>>>>>");
	   
        re.end();
	    System.out.println("r engine end");
	    
	    double contribution = 0;
	    int count = 0;
	    for (int i = 0; i < pcaContributionArray.length; i++){
	    	contribution += pcaContributionArray[i];
	    	if (contribution > 0.9){
	    		count = i;
	    		break;
	    	}
	    }
	    
	    //取前count列的特征向量，得到 7 * count的矩阵 
	    double[][] pcaResultTwoDimension = new double[EIGENVALUECOUNT][count+1];
	    for (int i = 0; i < EIGENVALUECOUNT; i++){
	    	for (int j = 0; j < count + 1; j++){
	    		if (i * 7 + j < pcaResultArray.length){
	    			pcaResultTwoDimension[i][j] = pcaResultArray[i*7+j];
	    		}
	    	}
	    }
	    //打印前index列的pca predict特征值结果
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
	    
	    MP.println("final data is:");
		MP.println(finalData);
		
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
