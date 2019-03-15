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

## hive commands

hive -e "use practical_exercise_1;"
validate_execution $? "use practical_exercise_1 Database"

hive -e "show tables;"
validate_execution $? "see the tables of the practical_exercise_1 Database "

## see the data of the tables / Content of the table 

hive -e "select * from practical_exercise_1.user;"
hive -e "select * from practical_exercise_1.activitylog;" 
hive -e "select * from practical_exercise_1.user_upload_dump;" 
validate_execution $? "see the tables of the practical_exercise_1 Database "


## generate user_report table and Content of the table

hive -e "insert overwrite table practical_exercise_1.user_report Select u.id, al.total_update, al.total_insert, al.total_delete, b.Type as Last_ACTIVITY_Type, al.IS_ACTIVE, d.total_upload from practical_exercise_1.user u left join (select row_number() over(partition by user_id order by timestamp desc) as row_num,*  from practical_exercise_1.activitylog) as b On u.id= b.user_id and b.row_num=1 join (select count(*) as total_upload, user_id  from practical_exercise_1.user_upload_dump group by user_id) as d on d.user_id=u.id join (select user_id, count(case when type='INSERT' then 1 else NULL end)as total_insert, count(case when type='UPDATE' then 1 else NULL end) as total_update, count(case when type='DELETE' then 1 else NULL end) as total_delete, (Case when max(from_unixtime(timestamp)) >= date_sub(current_timestamp,2) then 'true' else 'false' end) as IS_ACTIVE from practical_exercise_1.activitylog group by user_id) as al On u.id = al.user_id ;"

hive -e "select * from practical_exercise_1.user_report;"

validate_execution $? "generated user_report table"

  

