
hive -e "drop database practical_exercise_1 CASCADE;"

hive -e "show databases;" 

hadoop fs -rmr  /user/cloudera/workshop/exercise1

sqoop job \
--meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop \
--delete practical_exercise_1.activitylog

sqoop job \
--meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop \
--list

  






