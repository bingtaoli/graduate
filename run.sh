#!/bin/sh
hadoop fs -rmr /output/csv /output/csv2
hadoop jar rmscsv.jar rms/Rms  /data/csv/100 /output/csv /output/csv2
