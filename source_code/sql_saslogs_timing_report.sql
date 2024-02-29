
create table jianjundai_sas_log_raw as 
select  * 
from jianjundai_sas_log
;

create table jianjundai_sas_log_detail as 
with t1 as (
select 
    substr(log_text, 1, 14) as log_directory_name 
  , log_text
  , substr(log_text, 15)  as rest_text 
from jianjundai_sas_log_raw
)
, t2 as (
select
    log_directory_name
  , log_text
  , substr(rest_text, 1, instr(rest_text, ':', 1, 1)-1) as log_file_name 
from t1
)
, t3 as (
select 
    log_text
  , log_directory_name
  , log_file_name
  , case 
        when instr(log_text, 'real time', 1, 1) > 0 then 'real time' 
        when instr(log_text, 'cpu time', 1, 1) > 0 then 'cpu time' 
        else 'unknown'
    end as execute_time_type 
  , case
        when instr(log_text, 'real time', 1, 1) > 0 then instr(log_text, 'real time', 1, 1) + 10 
        when instr(log_text, 'cpu time', 1, 1) > 0  then instr(log_text, 'cpu time', 1, 1)  + 11
        else -99
    end as timing_start_pos
from t2 
)
, t4 as (
select 
    log_text
  , log_directory_name
  , log_file_name
  , execute_time_type
  , substr(log_text, timing_start_pos) as timing_raw 
from t3 
)
, t5 as (
select 
    log_text
  , log_directory_name
  , log_file_name
  , execute_time_type
  , replace(timing_raw, ' ', '') as timing_raw 
  -- , regexp_replace(timing_raw, '[0-9.]', '') as timing_type
from t4 
)
, t6 as (
select 
    log_text
  , log_directory_name
  , log_file_name
  , execute_time_type
  , timing_raw
  , case 
        when instr(timing_raw, 'seconds')>0 
        then cast(replace(timing_raw, 'seconds', '') as number) 
        when length(timing_raw) - length(replace(timing_raw, ':')) = 1 
        then cast(substr(timing_raw, 1, instr(timing_raw, ':', 1, 1) -1 )*60 as number)
             + cast(substr(timing_raw, instr(timing_raw, ':', 1, 1)+1) as number)   
        when length(timing_raw) - length(replace(timing_raw, ':')) = 2
        then cast(substr(timing_raw, 1, instr(timing_raw, ':', 1, 1) -1) * 3600 as number) 
             + cast(substr(timing_raw, instr(timing_raw, ':', 1, 1) + 1, instr(timing_raw, ':', 1, 2) - instr(timing_raw, ':', 1, 1)-1)*60 as number)
             + cast(substr(timing_raw, instr(timing_raw, ':',1, 2)+1) as number)  
        else null 
    end as time_in_seconds 
from t5
)
, t7 as (
select 
    log_text
  , log_directory_name
  , log_file_name
  , execute_time_type
  , time_in_seconds
from t6 
-- where instr(timing_raw, ':') = 1 
-- where time_in_seconds is null 
-- where log_text = '/saslogs/lev1/DATAMART_ECL_CME_01_DATAMART_ECL_L_TCK_MAP1_2024.01.24_07.01.51.758.0.log:      real time           2:58.17'
-- where log_text = '/saslogs/lev1/DATAMART_ECL_CME_01_DATAMART_ECL_L_ACR_MAP1_2024.02.07_07.01.23.829.0.log:      real time           1:06:36.42'
)
, t8 as (
select 
    log_directory_name
  , log_file_name
  , max(case when execute_time_type = 'real time' then time_in_seconds else null end) as real_time_duration 
  , max(case when execute_time_type = 'cpu time' then time_in_seconds else null end) as cpu_time_duration 
from t7
group by log_directory_name
  , log_file_name
)
select 
    log_directory_name
  , log_file_name
  , real_time_duration
  , cpu_time_duration
  , to_date(substr(log_file_name, instr(log_file_name, '2024.', 1, 1), 10), 'YYYY.MM.DD') as job_run_date
from t8 
-- where instr(log_file_name, '2024.', 1, 1) > 0 
;

select 
    job_run_date
  , max(real_time_duration)
  , max(cpu_time_duration)
from jianjundai_sas_log_detail
group by job_run_date
order by 1 
;

select * from jianjundai_sas_log_detail where real_time_duration > 36000
order by job_run_date desc 
;
