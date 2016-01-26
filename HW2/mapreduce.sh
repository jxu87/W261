## mapreduce.sh
## Author: Jing Xu
## Description: mapreduce bash script for HW2.5

hdfs dfs -mkdir /user #create hdfs folder
wait
hdfs dfs -mkdir /user/jing #create hdfs folder
wait
hadoop fs -put enronemail_1h.txt /user/jing #upload local file to hdfs folder
wait

#hadoop streaming mapreduce command 
hadoop jar /Users/JingXu/Documents/hadoop-2.6.3/share/hadoop/tools/lib/hadoop-streaming-2.6.3.jar \
-file mapper.py    -mapper mapper.py \
-file reducer.py   -reducer reducer.py \
-input /user/jing/* -output /user/jing/test-output1/ -file 'allowed_words'

hadoop fs -get /user/jing/test-output1/part-00000 hw2-5results #download hw2-5results to local
wait

hdfs dfs -rmr /user/jing #remove hdfs folder
hdfs dfs -rmr /user #remove hdfs folder
#rm hw2-3results #delete results 