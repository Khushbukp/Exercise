## this is the variable will hold record code the command if it fails. default its value is 0, means success.
## If this variable value is zero, means that all the commands in this script executed successfully

finalReturnCode=0
validate_execution()
{
  # Function. Argument 1 is the return code
  # 	      Argument 2 is text to display on Success/Failure message.
  #  uncomment the statement exit${1} if the requirement is to exit the flow when any error occurs.

  
  if [ "${1}" -ne "0" ]; 
  then
	echo " +++ ERROR +++ ############# Return Code :- ${1} : ${2}"
	finalReturnCode=${1}
	exit ${1}   ## enable this if the requirement is to exit the flow when any error occurs.

   else
	echo "++++ SUCCESS +++ ############# Return Code :- ${1} : ${2}"
  fi
}

## Droping database 
hive -e "drop database practical_exercise_1 CASCADE;"

## show database 
hive -e "show databases;" 

## delete the directory 
hadoop fs -rmr  /user/cloudera/workshop/exercise1

## deleting sqoop job 
sqoop job \
--meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop \
--delete practical_exercise_1.activitylog

## listing the sqoop jobs
sqoop job \
--meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop \
--list

  






