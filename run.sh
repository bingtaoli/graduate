#!/bin/sh
hadoop fs -rmr /output/csv /output/csv1 /output/csv2 /output/csv3 /output/csv4
# six args are needed
hadoop jar rmscsv.jar rms/Rms  /data/csv/100 \
/output/csv /output/csv1 /output/csv2 /output/csv3 /output/csv4
