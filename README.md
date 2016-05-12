> 毕设代码仓库

主要任务是预测轴承的寿命，具体的数学建模算法在业界比较成熟。

本毕业设计使用hadoop平台跑算法，支持并行处理、分布式、对超大文件的支持。

## map reduce

**PCA处理**:

FirstMapper: 把csv文件读入，输出`<time, double>`，time为int，但是作为str，double为倒数第二列的值。

FirstReducer: 得到`<time, iterator<double>>`，处理iterator，得到倒数第二列，处理得到7个特征值，输出`<time, eigens-str>`

SecondMapper: 把第一个job的输出文件读入，输出`<"second", eigen-arr-writable>`

SecondReducer: 输入`<"second", iterator<eigen-arr-writable>>`，输出pca。

**EMD处理**:

FirstMapper: 把csv文件读入，输出`<time, double>`，time为int，但是作为str。double为倒数第二列的值。

FirstReducer: 得到`<time, iterator<double>>`，处理iterator，得到倒数第二列，进行`emd`处理。得到imf，再求边际谱。

## jobs

由于步骤比较多，并且需要层层向下，所以采用多个job来实现。

#### 第一个job

读入csv，采用mapper采用CombineFileInputFormat这种input格式，否则文件小并且数目极多会造成很大的性能消耗。mapper中每次读取一行的倒数第二列的值，得到一个iterator传入reducer。reducer先踢除奇异点，再求出7个特征值(分别是均方根,峰峰值,波形指数,峰值指标,脉冲指标,裕度因子,峭度因子)。

#### 第二个job

对第一个job得到的7个特征值进行pca降维。

## 调用RJava进行数据处理

使用rjava进行数据处理，比如PCA

## R分析结果

比如分析pca的走势，在secondJob中把R处理得到的pca结果输出到了HDFS中的一个csv文件。我们使用R去绘图查看走势。`read.csv(file, header = FALSE)`

```r
pca = read.csv('/Users/libingtao/graduate/pcaresult.txt', header=FALSE)
# 现在有2803个点
x <- 1:2803
plot(x, pca[,1])
```