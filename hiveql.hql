-- hive commands
use practical_exercise_1;

-- insert data into user_report table 
insert overwrite table practical_exercise_1.user_report Select u.id, al.total_update, al.total_insert, al.total_delete, b.Type as Last_ACTIVITY_Type, al.IS_ACTIVE, d.total_upload from practical_exercise_1.user u left join (select row_number() over(partition by user_id order by timestamp desc) as row_num,*  from practical_exercise_1.activitylog) as b On u.id= b.user_id and b.row_num=1 join (select count(*) as total_upload, user_id  from practical_exercise_1.user_upload_dump group by user_id) as d on d.user_id=u.id join (select user_id, count(case when type='INSERT' then 1 else NULL end)as total_insert, count(case when type='UPDATE' then 1 else NULL end) as total_update, count(case when type='DELETE' then 1 else NULL end) as total_delete, (Case when max(from_unixtime(timestamp)) >= date_sub(current_timestamp,2) then 'true' else 'false' end) as IS_ACTIVE from practical_exercise_1.activitylog group by user_id) as al On u.id = al.user_id ; 

-- see the data of user_report table 
select * from practical_exercise_1.user_report; 

--inser data into user_total table 

insert into practical_exercise_1.user_total select current_timestamp,a.cnt,a.cnt-coalesce(b.total_users,0) from (select 1 c,count(*) cnt  from practical_exercise_1.user) a left join ( select 1 d ,total_users,user_added,time_ran from practical_exercise_1.user_total order by time_ran desc limit 1) b on a.c=b.d;

-- see the data of user_total table
select * from practical_exercise_1.user_total;
