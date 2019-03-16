## this is the variable will hold record code the command if it fails. default its value is 0, means success.
## If this variable value is zero, means that all the commands in this script executed successfully

validate_execution()
{
  # Function. Argument 1 is the return code
  # 	      Argument 2 is text to display on Success/Failure message.
  #  uncomment the statement exit${1} if the requirement is to exit the flow when any error occurs.

  
  if [ "${1}" -ne "0" ]; 
  then
	echo " +++ ERROR +++ ############# Return Code :- ${1} : ${2}"
	exit ${1}   ## enable this if the requirement is to exit the flow when any error occurs.
   else
	echo "++++ SUCCESS +++ ############# Return Code :- ${1} : ${2}"
  fi
}

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
validate_execution $? "Listing the content of  direcotry /user/cloudera/workshop/"
  

hive -e "CREATE EXTERNAL TABLE if not exists practical_exercise_1.user_upload_dump ( user_id int, file_name STRING, timestamp int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/cloudera/workshop/exercise1/' tblproperties ('skip.header.line.count' = '1');"
validate_execution $? "Creating hive external table user_upload_dump"


hive -e "create table if not exists practical_exercise_1.user_report(id int, total_update bigint, total_insert bigint, total_delete bigint, last_activity_type string, is_active boolean, upload_count bigint);"
validate_execution $? "Creating hive table user_report"


hive -e "CREATE TABLE if not exists practical_exercise_1.user_total (time_ran TIMESTAMP, total_users int, user_added int);" 
validate_execution $? "Creating hive table user_total"




