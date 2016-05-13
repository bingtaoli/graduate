library(EMD)
library(signal)

ampfreq <- read.csv('/Users/libingtao/graduate/emdresult.txt', header=FALSE)
amplitude <- ampfreq[,1]
insfreq <- ampfreq[,2]
originDataLength <- length(amplitude)
#现在的频率是0~1之间，可能是经过了归一化
#首先把频率放大为0~400
bigger <- 400 / max(insfreq, na.rm=TRUE) #去除NA的干扰
bigger <- floor(bigger) #向下取整
for (i in 1:originDataLength){
	if (is.na(insfreq[i])) {
		insfreq[i] = 0
	}
	insfreq[i] = floor(insfreq[i] * bigger)
}
#构建一个涵盖两个矩阵信息的矩阵
toimageResult <- matrix(data=0, nrow=400, ncol=originDataLength)
for (i in 1:originDataLength){
    freq <- insfreq[i]
    toimageResult[freq, i] = amplitude[i]
}
#求sum
bjp <- array(1:400)
for (i in 1:400){
    sum <- sum(toimageResult[i,], na.rm=TRUE)
    bjp[i] = sum
}

xx <- 1:400

plot(xx, bjp, type='l')

hir <- interp1(xx, bjp, 221)
hor <- interp1(xx, bjp, 168)
hbr <- interp1(xx, bjp, 215.48)