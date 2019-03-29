## Importing the utility function from file Exercise_Utility_Functions which contains method called validate_execution

. /home/cloudera/heena/Exercise_utility_function

## create database  practical_exercise_1
hive -e "create database practical_exercise_1;" 
validate_execution $? "Hive -e create database practical_exercise_1"

## import data from the MySQL Tables into Hive(Activitylog)
sqoop job \
--meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop \
--create practical_exercise_1.activitylog \
-- import \
--connect jdbc:mysql://localhost/practical_exercise_1 \
--username root \
--password-file /user/cloudera/root_pwd.txt \
--table activitylog \
-m 2 \
--hive-import \
--hive-database practical_exercise_1 \
--hive-table activitylog \
--incremental append \
--check-column id \
--last-value 0
validate_execution $? "Sqoop job for importing data for activity log"

## make directory to hdfs 

hadoop fs -mkdir /user/cloudera/workshop/exercise1
validate_execution $? "Hadoop creating direcotry /user/cloudera/workshop/exercise1"

echo "let's see the content of workshop directory"
hadoop fs -ls  /user/cloudera/workshop/ 


hive -e "CREATE EXTERNAL TABLE if not exists practical_exercise_1.user_upload_dump ( user_id int, file_name STRING, timestamp int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/cloudera/workshop/exercise1/' tblproperties ('skip.header.line.count' = '1');"
validate_execution $? "Creating hive external table user_upload_dump"


hive -e "create table if not exists practical_exercise_1.user_report(id int, total_update bigint, total_insert bigint, total_delete bigint, last_activity_type string, is_active boolean, upload_count bigint);"
validate_execution $? "Creating hive table user_report"


hive -e "CREATE TABLE if not exists practical_exercise_1.user_total (time_ran TIMESTAMP, total_users int, user_added int);" 
validate_execution $? "Creating hive table user_total"




