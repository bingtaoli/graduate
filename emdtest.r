library(EMD)
library(hht)

source <- read.csv('/Users/libingtao/graduate/acc_00004.csv', header=FALSE)
data <- source[,5]
datalength <- length(data)
xx <- 1 : datalength
result <- emd(data, xx, boundary="none")  # a little slow, about 2500ms
result2 <- Sig2IMF(data, xx)