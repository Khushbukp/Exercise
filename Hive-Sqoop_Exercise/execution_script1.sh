## Importing the utility function from file Exercise_Utility_Functions which contains method called validate_execution

. /home/cloudera/heena/Exercise_utility_function

## executing python command to generate data
python /home/cloudera/practical_exercise_data_generator.py --load_data

python /home/cloudera/practical_exercise_data_generator.py --create_csv

## executing sqoop job
sqoop job \
--meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop \
--exec practical_exercise_1.activitylog 
validate_execution $? "executing sqoop job"

## overwrititng user data  
sqoop import \
--connect jdbc:mysql://localhost/practical_exercise_1 \
--username root \
--password-file /user/cloudera/root_pwd.txt \
--table user \
-m 2 \
--hive-import \
--hive-overwrite \
--hive-database practical_exercise_1 \
--hive-table user 
validate_execution $? "overwriting user data"


## To ingest CSV files from the local file system into HDFS
i=0
for file in *.csv
do
 echo "file name before appending time stamp --> $file"
 filename=$(cut -d'.' -f1 <<<"$file")
 mv $file /home/cloudera/TEMP/$filename.$(date +%s)$i.csv
validate_execution $? "moving csv file to TEMP folder"
i=$(($i+1))
done
validate_execution $? "moving all csv files to TEMP folder"

hadoop fs -put   /home/cloudera/TEMP/*.csv   /user/cloudera/workshop/exercise1/  
validate_execution $? "copying all csv files from local system to hdfs"
  
hadoop fs -ls  /user/cloudera/workshop/exercise1/
validate_execution $? "seeing the content of the directory exercise1"

mv /home/cloudera/TEMP/*.csv   /home/cloudera/backup/
validate_execution $? "check all .csv files moved to backup folder or not"



