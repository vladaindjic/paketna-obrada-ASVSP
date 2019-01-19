DIR=/complaints

cd $DIR

printf "\nCOPY FILE TO HDFS\n"

$HADOOP_PREFIX/bin/hdfs dfs -put /complaints/complaints.csv /complaints

printf "\nSETTING EXECUTEABLE PY\n"

chmod a+x *.py

cd $HADOOP_PREFIX

printf "\nRUN HADOOP-STREAMING\n"

time bin/hadoop jar share/hadoop/tools/lib/hadoop-streaming-$HADOOP_VERSION.jar \
    -input /complaints \
    -output /complaints_out \
    -mapper $DIR/mapper.py \
    -reducer $DIR/reducer.py

printf "\nRESULTS\n"

$HADOOP_PREFIX/bin/hdfs dfs -cat /complaints_out/*
