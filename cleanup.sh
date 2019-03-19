## Importing the utility function from file Exercise_Utility_Functions which contains method called validate_execution

. /home/cloudera/heena/Exercise_utility_function

## Droping database 
hive -e "drop database practical_exercise_1 CASCADE;"

## show database 
hive -e "show databases;"
 
## delete the directory 
hadoop fs -test -e /user/cloudera/workshop/exercise1

if [ "$?" -eq 0 ]
then
echo "directory is exist"
hadoop fs -rmr  /user/cloudera/workshop/exercise1
validate_execution $? "deleting the exercise1 directory"
else
echo "directory isn't exist"
fi

## deleting sqoop job 
(sqoop job --meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop --list) | grep 'practical_exercise_1.activitylog' &> /dev/null 
validate_execution $? "Conforming sqoop job"

if [ $? == 0 ];
 then 
    echo "matched"
	## deleting sqoop job if the name is exist
	sqoop job \
	--meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop \
	--delete practical_exercise_1.activitylog
  validate_execution $? "deleting the sqoop job"
fi

## listing the sqoop jobs
sqoop job \
--meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop \
--list
validate_execution $? "listing the all sqoop jobs"








