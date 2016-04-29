package algorithm;

import java.util.Arrays;

import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.stat.correlation.Covariance;
import org.ujmp.core.DenseMatrix;
import org.ujmp.core.Matrix;

import utils.MP;

/**
 * 将样本点投影到选取的特征向量上。假设样例数为m，特征数为n，减去均值后的样本矩阵为DataAdjust(m*n)，协方差矩阵是n*n，
 * 选取的k个特征向量组成的矩阵为EigenVectors(n*k)。那么投影后的数据FinalData为:
 * FinalData(m * k) = DataAdjust(m * n) X EigenVectors(n * k)
 * 这样，就将原始样例的n维特征变成了k维，这k维就是原始特征在k维上的投影。
 * 比如k是1，m是10的话，最后得到10行1列的矩阵。
 */
public class Pca {
	
	private static double PRECISE = 1;
	
	/**
	 * pca主要是三步
	 * http://www.cnblogs.com/zhangchaoyang/articles/2222048.html
	 * 1. 特征中心化。即每一维的数据都减去该维的均值，这里的“维”指的就是一个特征（或属性），变换之后每一维的均值都变成了0。
	 * 2. 协方差矩阵
	 * 3. 特征值和特征向量
	 * @param array
	 * @return
	 */
	public static double[][] calculate(double[][] array){
		
		//MP.closeDebug();
		
		//1. 特征中心化。即每一维的数据都减去该维的均值
		for (int i = 0; i < array.length; i++){
			double sum = 0;
			for (int j = 0; j < array[i].length; j++){
				sum += array[i][j];
			}
			double average = sum / array[i].length;
			for (int j = 0; j < array[i].length; j++){
				array[i][j] -= average;
			}
		}
		
		Matrix m = Matrix.Factory.linkToArray(array);
		
		//2. 协方差矩阵
		MP.println("协方差矩阵: ");
		Covariance cov = new Covariance(array);
		
		RealMatrix covResult = cov.getCovarianceMatrix();
		MP.println(covResult);
		MP.println();
		
		/**
		 * NOTE
		 * 使用ujmp求eigenValueDecomposition
		 * 使用math3包中提供的EigenDecomposition求出的结果和matlab不一致，使用ujmp库则一致
		 */
		//Array2DRowRealMatrix的内部实现是一个2维double类型的数组double [][]data
		//它的getData方法可以返回对应的数组表示
		double[][] covArray = covResult.getData();
		Matrix covMatrix = Matrix.Factory.linkToArray(covArray);
		Matrix[] eigenValueDecomposition = covMatrix.eig();
		//如果长度小于2则报错
		try {
			if (eigenValueDecomposition.length < 2){
				throw new Exception("求特征向量矩阵出错!");
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		
		//3. 特征值和特征向量
		Matrix V = eigenValueDecomposition[0];
		Matrix D = eigenValueDecomposition[1];
		
		/**
		 * a. 特征值按照从小到大排序
		 * b. 选取前k个特征值,达到RRECISE，比如80%
		 * c. 将其对应的k个特征向量分别作为列向量组成特征向量矩阵。
		 */
		int rows = (int)D.getRowCount();
		int cols = (int)D.getColumnCount();
		double[] ans = new double[rows];
		for (int i = 0; i < rows; i++){
			//initialize
			ans[i] = 0;
			for (int j = 0; j < cols; j++){
				if (D.getAsDouble(i, j) != 0){
					ans[i] = D.getAsDouble(i, j);
				}
			}
		}
		Arrays.sort(ans);
		
		MP.println("ans is:");
		for (int i = 0; i < ans.length; i++){
			MP.println(ans[i]);
		}
		
		MP.println();
		double sum = 0;
		for (int i = 0; i < ans.length; i++){
			sum += ans[i];
		}
		//得到前k个特征量
		int k = 0;
		double rate = 0;
		for (int i = ans.length - 1; i >= 0; i--){
			rate += ans[i] / sum;
			if (rate >= PRECISE){
				k = i;
				break;
			}
		}
		//取对应的部分特征向量
		int vCols = (int)V.getColumnCount();
		int vRows = (int)V.getRowCount();
		double[][] kd = new double[ans.length-k][vCols];
		for (int i = k; i <= vRows - 1 ; i++){
			for (int j = 0; j < vCols; j++){
				kd[i-k][j] = V.getAsDouble(i, j);
			}
		}
		
		//kd变换为矩阵
		Matrix km = Matrix.Factory.linkToArray(kd);
		//转置，参见http://www.cnblogs.com/jerrylead/archive/2011/04/18/2020209.html
		//原来是k*m， 转变成m*k矩阵，所以最终结果才能是m行k列的矩阵
		km = km.transpose();

		Matrix finalData = DenseMatrix.Factory.zeros(m.getRowCount(), k);
		finalData = m.mtimes(km);
		MP.println("final data is:");
		MP.println(finalData);
		
		double[][] result = finalData.toDoubleArray();
		
		MP.debug();
		
		return result;
	}
	
}
