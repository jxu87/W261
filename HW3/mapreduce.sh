## mapreduce.sh
## Author: Jing Xu
## Description: mapreduce bash script for HW3.2.2

hdfs dfs -mkdir /user # create hdfs folder
wait
hdfs dfs -mkdir /user/jing # create hdfs folder
wait
hadoop fs -put Consumer_Complaints.csv /user/jing # upload local file to hdfs folder
wait

# hadoop command to run streaming mapreduce job
#sort primarly by numeric descending in 2nd key, then alphabetically on 1st key
hadoop jar /Users/JingXu/Documents/hadoop-2.6.3/share/hadoop/tools/lib/hadoop-streaming-2.6.3.jar \
-D mapred.job.name="Count Job via Streaming" \
-D mapred.map.tasks=2 \
-D mapred.reduce.tasks=2 \
-D stream.num.map.output.key.fields=2 \
-D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
-D mapred.text.keycomparator.options="-k2,2nr -k1,1" \
-file mapper.py    -mapper mapper.py \
-file reducer.py   -reducer reducer.py \
-input /user/jing/* -output /user/jing/test-output1/
wait

hadoop dfs -cat /user/jing/test-output1/part-00000
wait

hdfs dfs -rmr /user/jing # remove hdfs folder
hdfs dfs -rmr /user # remove hdfs folder