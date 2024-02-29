/* 

-- SAS execution timing log -- JIANJUNDAI_SAS_LOG_RAW
1 location of the sas log /saslogs/
2 extract run time from the log 
3 import the log into table 

-- IBM LSF log -- JIANJUNDAI_LSF_LOG_RAW
1 location of the sas log /saslogs/
2 extract run time from the log 
3 import the log into table 

*/










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


--select * from jianjundai_sas_log_detail where real_time_duration > 36000
--order by job_run_date desc 
--;


--select * from jianjundai_sas_log_detail
--where log_file_name like 'DATAMART_ECL_PACE_DATAMART_ECL_L_PCS_MAP1_%'
--order by job_run_date 
--;


-- select count(*) from jianjundai_lsf_log_raw;

create table jianjundai_lsf_log_raw_bkup as select * from jianjundai_lsf_log_raw;

drop table jianjundai_lsf_log_raw ;
create table jianjundai_lsf_log_raw (
  col_0a  varchar2(1000)
, col_0b  varchar2(1000)
, col_0c  varchar2(1000)
, col_0d  varchar2(1000)
, col_0e  varchar2(1000)
, col_0f  varchar2(1000)
, col_0g  varchar2(1000)
, col_0h  varchar2(1000)
, col_0i  varchar2(1000)
, col_0j  varchar2(1000)
, col_0k  varchar2(1000)
, col_0l  varchar2(1000)
, col_0m  varchar2(1000)
, col_0n  varchar2(1000)
, col_0o  varchar2(1000)
, col_0p  varchar2(1000)
, col_0q  varchar2(1000)
, col_0r  varchar2(1000)
, col_0s  varchar2(1000)
, col_0t  varchar2(1000)
, col_0u  varchar2(1000)
, col_0v  varchar2(1000)
, col_0w  varchar2(1000)
, col_0x  varchar2(1000)
, col_0y  varchar2(1000)
, col_0z  varchar2(1000)
, col_aa  varchar2(1000)
, col_ab  varchar2(1000)
, col_ac  varchar2(1000)
, col_ad  varchar2(1000)
, col_ae  varchar2(1000)
, col_af  varchar2(1000)
, col_ag  varchar2(1000)
, col_ah  varchar2(1000)
, col_ai  varchar2(1000)
, col_aj  varchar2(1000)
, col_ak  varchar2(1000)
, col_al  varchar2(1000)
, col_am  varchar2(1000)
, col_an  varchar2(1000)
, col_ao  varchar2(1000)
, col_ap  varchar2(1000)
, col_aq  varchar2(1000)
, col_ar  varchar2(1000)
, col_as  varchar2(1000)
, col_at  varchar2(1000)
, col_au  varchar2(1000)
, col_av  varchar2(1000)
, col_aw  varchar2(1000)
, col_ax  varchar2(1000)
, col_ay  varchar2(1000)
, col_az  varchar2(1000)
, col_ba  varchar2(1000)
, col_bb  varchar2(1000)
, col_bc  varchar2(1000)
, col_bd  varchar2(1000)
, col_be  varchar2(1000)
, col_bf  varchar2(1000)
, col_bg  varchar2(1000)
, col_bh  varchar2(1000)
, col_bi  varchar2(1000)
, col_bj  varchar2(1000)
, col_bk  varchar2(1000)
, col_bl  varchar2(1000)
, col_bm  varchar2(1000)
, col_bn  varchar2(1000)
, col_bo  varchar2(1000)
, col_bp  varchar2(1000)
, col_bq  varchar2(1000)
, col_br  varchar2(1000)
, col_bs  varchar2(1000)
, col_bt  varchar2(1000)
, col_bu  varchar2(1000)
, col_bv  varchar2(1000)
, col_bw  varchar2(1000)
, col_bx  varchar2(1000)
, col_by  varchar2(1000)
, col_bz  varchar2(1000)
, col_ca  varchar2(1000)
, col_cb  varchar2(1000)
, col_cc  varchar2(1000)
, col_cd  varchar2(1000)
, col_ce  varchar2(1000)
, col_cf  varchar2(1000)
, col_cg  varchar2(1000)
, col_ch  varchar2(1000)
, col_ci  varchar2(1000)
, col_cj  varchar2(1000)
, col_ck  varchar2(1000)
, col_cl  varchar2(1000)
, col_cm  varchar2(1000)
, col_cn  varchar2(1000)
, col_co  varchar2(1000)
, col_cp  varchar2(1000)
, col_cq  varchar2(1000)
, col_cr  varchar2(1000)
, col_cs  varchar2(1000)
, col_ct  varchar2(1000)
, col_cu  varchar2(1000)
, col_cv  varchar2(1000)
, col_cw  varchar2(1000)
, col_cx  varchar2(1000)
, col_cy  varchar2(1000)
, col_cz  varchar2(1000)
, col_da  varchar2(1000)
)
;
create table jianjundai_lsf_log_detail_bkup as select * from jianjundai_lsf_log_detail;
drop table jianjundai_lsf_log_detail;
create table jianjundai_lsf_log_detail as 
with t1 as (
select  
    col_0c as job_start_time_epoch 
  , col_ab as lsf_process_flow_string
  , col_ac as lsf_job_string
  , length(col_ab) - length(replace(col_ab, ':')) as lsf_process_flow_str_colon_cnt 
--   , col_bo
from jianjundai_lsf_log_raw
)
-- select lsf_process_flow_str_colon_cnt, count(*) from t1 group by lsf_process_flow_str_colon_cnt order by 1 ;
, t2 as (
select 
    job_start_time_epoch
  , lsf_process_flow_str_colon_cnt
  , trunc(cast(TIMESTAMP '1970-01-01 00:00:00' + (job_start_time_epoch / (24*60*60)) as date)) as job_start_date
  , cast(TIMESTAMP '1970-01-01 00:00:00' + (job_start_time_epoch / (24*60*60)) as date) as job_start_time
  , substr(lsf_process_flow_string, 1, instr(lsf_process_flow_string, ':', 1, 1)-1) as lsf_id
  , substr(lsf_process_flow_string, instr(lsf_process_flow_string, ':', 1, 1)+1, (instr(lsf_process_flow_string, ':', 1, 2) - instr(lsf_process_flow_string, ':', 1, 1) -1 ) ) as lsf_user
  , substr(lsf_process_flow_string, instr(lsf_process_flow_string, ':', 1, 2)+1, (instr(lsf_process_flow_string, ':', 1, 3) - instr(lsf_process_flow_string, ':', 1, 2) -1 ) ) as lsf_process_flow_name
  , case 
        when lsf_process_flow_str_colon_cnt = 3 then substr(lsf_process_flow_string, instr(lsf_process_flow_string, ':', 1, 3)+1, (length(lsf_process_flow_string) - instr(lsf_process_flow_string, ':', 1, 3) ))
        when lsf_process_flow_str_colon_cnt > 3 then substr(lsf_process_flow_string, instr(lsf_process_flow_string, ':', 1, 3)+1, (instr(lsf_process_flow_string, ':', 1, 4) - instr(lsf_process_flow_string, ':', 1, 3) -1 ))
        else null 
    end as lsf_part_4 
  , case 
        when lsf_process_flow_str_colon_cnt = 4 then substr(lsf_process_flow_string, instr(lsf_process_flow_string, ':', 1, 4)+1, (length(lsf_process_flow_string) - instr(lsf_process_flow_string, ':', 1, 4) ))
        when lsf_process_flow_str_colon_cnt > 4 then substr(lsf_process_flow_string, instr(lsf_process_flow_string, ':', 1, 4)+1, (instr(lsf_process_flow_string, ':', 1, 5) - instr(lsf_process_flow_string, ':', 1, 4) -1 ))
        else null 
    end as lsf_part_5
  , case 
        when lsf_process_flow_str_colon_cnt = 5 then substr(lsf_process_flow_string, instr(lsf_process_flow_string, ':', 1, 5)+1, (length(lsf_process_flow_string) - instr(lsf_process_flow_string, ':', 1, 5) ))
        else null 
    end as lsf_part_6
  , lsf_process_flow_string
  , lsf_job_string
from t1
-- where lsf_process_flow_str_colon_cnt = 5 
-- where lsf_job_string like '%UDS_ADDRESS_01%'
)
, t3 as (
select 
    job_start_date
  , job_start_time
  , lsf_id 
  , lsf_user
  , lsf_process_flow_name
  , lsf_process_flow_str_colon_cnt
  , lsf_part_4 
  , lsf_part_5
  , lsf_part_6
  , case 
      when lsf_part_6 is not null then lsf_part_5
      when lsf_part_6 is null and lsf_part_5 is not null then lsf_part_4
      when lsf_part_6 is null and lsf_part_5 is null and lsf_part_4 is not null then lsf_process_flow_name
      else null 
    end as lsf_subprocess_name 
  , case 
      when lsf_part_6 is not null then lsf_part_6
      when lsf_part_6 is null and lsf_part_5 is not null then lsf_part_5
      when lsf_part_6 is null and lsf_part_5 is null and lsf_part_4 is not null then lsf_part_4
      else null 
    end as lsf_job_name 
  , lsf_process_flow_string 
  , lsf_job_string
  , substr(lsf_job_string, instr(lsf_job_string, '-sysin', 1, 1) +7 ) as lsf_sas_code_name
  , substr(lsf_job_string, instr(lsf_job_string, '-log', 1, 1) +5, (instr(lsf_job_string, '-batch', 1, 1) -1 - instr(lsf_job_string, '-log', 1, 1) - 5 )) as lsf_log_file_full_name
from t2 
)
select 
 job_start_date
  , job_start_time
  , lsf_id 
  , lsf_user
  , lsf_process_flow_name
  , lsf_subprocess_name
  , lsf_job_name
  , lsf_sas_code_name
  , lsf_log_file_full_name
  , substr(lsf_log_file_full_name, 1, 14) as lsf_log_file_path_name
  , replace(replace(replace(trim(lsf_log_file_full_name), '/saslogs/lev3/' , ''), '/saslogs/lev1/' , ''), '#Y.#m.#d_#H.#M.#s.log' , '') as lsf_log_file_id
from t3   
;

-- select count(*) from jianjundai_lsf_log_detail;
-- 'DW_FINANCE_EDW_STAG_ALE_WUS_MAP1_#Y.#m.#d_#H.#M.#s.log'

-- jianjundai_lsf_sas_log_detail contains the join of LSF and SAS side of the information 
create table jianjundai_lsf_sas_log_detail as 
with saslog as (
select 
   log_directory_name
   , log_file_name
   , substr(log_file_name, 1, length(log_file_name) - 29) as log_file_id
   , real_time_duration
   , cpu_time_duration
   , job_run_date 
from jianjundai_sas_log_detail
)
, t1 as (
select
       -- lsflog.*
     lsflog.job_start_date 
   , lsflog.job_start_time
   , lsflog.lsf_id 
   , lsflog.lsf_user
   , lsflog.lsf_process_flow_name
   , lsflog.lsf_subprocess_name
   , lsflog.lsf_job_name
   , lsflog.lsf_sas_code_name
   , lsflog.lsf_log_file_path_name
   , lsflog.lsf_log_file_id 
   , saslog.log_file_name
   , saslog.real_time_duration
   , saslog.cpu_time_duration
from jianjundai_lsf_log_detail   lsflog 
left outer join saslog 
on lsflog.lsf_log_file_id = saslog.log_file_id 
where 1=1 
-- and lsflog.lsf_job_name like '%DATAMART_ECL_L_DPB_MAP1%'  
and trunc(lsflog.job_start_date) = trunc(saslog.job_run_date)
-- and trunc(lsflog.job_start_date) = to_date('05/02/2024', 'DD/MM/YYYY')
-- group by lsflog.lsf_sas_code_name
order by 1 
)
-- order by lsflog.job_start_date 
select *
from t1 
order by lsf_id, job_start_time 
;

-- DATAMART_ECL_CME_01_DATAMART_ECL_L_DPB_MAP1_2024.02.05_07.01.41.503.0.log

--select * 
--from jianjundai_lsf_log_detail
--where lsf_job_name like '%DATAMART_ECL_L_DPB_MAP1%'  
--order by job_start_date 
-- 'DATAMART_ECL_CME_01_DATAMART_ECL_L_DPB_MAP1_2024.02.05_07.01.41.503.0.log'


--select * 
--from jianjundai_sas_log_detail
--where log_file_name like 'DATAMART_ECL_CME_01_DATAMART_ECL_L_DPB_MAP1_%'




-- Load the SAS log - record count 
drop table jianjundai_sas_log_rcrdcnt_raw ;
create table jianjundai_sas_log_rcrdcnt_raw (
  log_file_name  varchar2(1000)
, note  varchar2(1000)
, col_0c  varchar2(1000)
, record_count_comment  varchar2(1000)
)
;

create table jianjundai_sas_log_rcrdcnt as 
with t1 as (
select log_file_name 
, record_count_comment
, case 
      when record_count_comment like ' 1 row was inserted%'    then '1'
      when record_count_comment like ' No rows were inserted%' then '0' 
      when record_count_comment like ' % rows were inserted%'  then substr(record_count_comment, 2, instr(record_count_comment, ' ', 1, 2)-2 ) 
      when record_count_comment like ' Table %'                then substr(record_count_comment, instr(record_count_comment, 'with ', 1, 1)+5, instr(record_count_comment, 'rows ', 1, 1) - instr(record_count_comment, 'with ', 1, 1) - 5 )
      else null 
  end as involved_record_count
, case 
      when record_count_comment like ' 1 row was inserted%'    then 'INSERT'
      when record_count_comment like ' No rows were inserted%' then 'INSERT' 
      when record_count_comment like ' % rows were inserted%'  then 'INSERT'
      when record_count_comment like ' Table %'                then 'CTAS'
      else null 
  end as involved_operation_type 
, case 
      when record_count_comment like ' 1 row was inserted%'    then substr(record_count_comment, instr(record_count_comment, 'into ', 1, 1)+5, length(record_count_comment) - instr(record_count_comment, 'into ', 1, 1) -5 ) 
      when record_count_comment like ' No rows were inserted%' then null 
      when record_count_comment like ' % rows were inserted%'  then substr(record_count_comment, instr(record_count_comment, 'into ', 1, 1)+5, length(record_count_comment) - instr(record_count_comment, 'into ', 1, 1) -5 ) 
      when record_count_comment like ' Table %'                then substr(record_count_comment, instr(record_count_comment, ' ', 1, 2)+1, instr(record_count_comment, ' ', 1, 3) - instr(record_count_comment, ' ',1 , 2))
      else null 
  end as involved_object_name 
from jianjundai_sas_log_rcrdcnt_raw
--where record_count_comment like ' 1 row was inserted%'
--or    record_count_comment like ' No rows were inserted%'
--or    record_count_comment like ' % rows were inserted%'
--or    record_count_comment like ' Table %'
)
select * 
from t1 
-- where log_file_name = '/saslogs/lev1/DW_STAG004_CPD_PRODUCT_STAG004_CPD_PRODUCT_01_EXT_2024.02.09_03.32.31.193.0.log'
-- select involved_record_count, count(*)
-- from t1
where involved_record_count is not null 
-- group by log_file_name 
-- order by 2 desc 
;

--select record_count_comment, count(*)
--from jianjundai_sas_log_rcrdcnt_raw
--where not (
--      record_count_comment like ' 1 row was inserted%'
--or    record_count_comment like ' No rows were inserted%'
--or    record_count_comment like ' % rows were inserted%'
--or    record_count_comment like ' Table %'
--)
--group by record_count_comment
--order by 1 
;

select * 
from jianjundai_lsf_sas_log_allinfo
where 1=1 
-- and total_ctas_insert_rcdcnt is not null
and job_start_date = to_date('06-Feb-2024', 'DD-Mon-YYYY')
-- group by job_start_date 
order by 1 
;

select *  
from jianjundai_sas_log_rcrdcnt_raw 
where log_file_name like '/saslogs/lev1/DATAMART_ECL_CME_01_DATAMART_ECL_D_ACM_MAP%'
order by 1 
;

drop table jianjundai_lsf_sas_log_allinfo;
create table jianjundai_lsf_sas_log_allinfo as 
with t1 as (
select 
     log_file_name
   , max(case when involved_operation_type = 'CTAS' then 'Y' else 'N' end) as CTAS_OPERATION_INVOLVED
   , max(case when involved_operation_type = 'INSERT' then 'Y' else 'N' end) as INSERT_OPERATION_INVOLVED
   , sum(involved_record_count) total_ctas_insert_rcdcnt
   , count(*)    total_ctas_insert_objcnt
   , listagg(involved_object_name, '|') within group(order by involved_object_name) as involved_object_names 
from jianjundai_sas_log_rcrdcnt
group by log_file_name 
)
-- select lg.job_start_date, case when cnt.log_file_name is not null then 'Y' else 'N' end as cnt_flg, count(*) 
, t2 as (
select 
     lg.job_start_date
   , lg.job_start_time
   , lg.lsf_id
   , lg.lsf_user
   , lg.lsf_process_flow_name
   , lg.lsf_subprocess_name
   , lg.lsf_job_name
   , lg.lsf_sas_code_name
   , lg.lsf_log_file_path_name
   , lg.lsf_log_file_id
   , lg.real_time_duration
   , lg.cpu_time_duration
   , cnt.log_file_name    as rcdcnt_file_name
   , cnt.ctas_operation_involved
   , cnt.insert_operation_involved
   , cnt.total_ctas_insert_rcdcnt
   , cnt.total_ctas_insert_objcnt 
   , cnt.involved_object_names
from JIANJUNDAI_LSF_SAS_LOG_DETAIL                  lg
left outer join t1                                  cnt 
on lg.lsf_log_file_path_name||lg.log_file_name = cnt.log_file_name 
-- where cnt.log_file_name is null 
-- group by job_start_date, case when cnt.log_file_name is not null then 'Y' else 'N' end 
order by 1, 2 
)
select *
from t2 
;

--select * from JIANJUNDAI_SAS_MERGE_JOBS;
--select * from jianjundai_sas_log_rcrdcnt;
--select * from JIANJUNDAI_LSF_SAS_LOG_DETAIL  
--group by job_start_date 
--order by 1 
--;
--select * 
--from jianjundai_sas_log_rcrdcnt
--;
--
--select * from dba_tables where table_name = 'SAS_JOBS_LOG';

--select 
--     log_file_name
--   , max(case when involved_operation_type = 'CTAS' then 'Y' else 'N' end) as CTAS_OPERATION_INVOLVED
--   , max(case when involved_operation_type = 'INSERT' then 'Y' else 'N' end) as INSERT_OPERATION_INVOLVED
--   , sum(involved_record_count) total_ctas_insert_rcdcnt
--   , count(*)    total_ctas_insert_objcnt
--from jianjundai_sas_log_rcrdcnt
--group by log_file_name 
---- having count(*) <> 1 
--order by 1 desc 
--;
--
--select distinct involved_operation_type
--from jianjundai_sas_log_rcrdcnt
---- where log_file_name = '/saslogs/lev1/DW_CME_PERSIST_DIM_RATEABLE_LOCATION_UDS_DIM_RATEABLE_LOCATION_01_2024.02.09_06.05.52.651.0.log'
--


-- Load merge 
drop table JIANJUNDAI_SAS_MERGE_JOBS_RAW;
create table JIANJUNDAI_SAS_MERGE_JOBS_RAW
( 
  LOG_FILE_NAME varchar2(4000),
  LOG_FILE_FULL_PATH varchar2(4000),
  LOG_TABLE_DETAIL varchar2(4000)
);

drop table JIANJUNDAI_SAS_MERGE_JOBS;
create table JIANJUNDAI_SAS_MERGE_JOBS as 
with t1 as (
select 
     distinct
     log_file_name                as mrg_log_file_name
   -- , log_file_full_path           as mrg_log_file_full_path
   , log_table_detail             as mrg_log_table_detail
from JIANJUNDAI_SAS_MERGE_JOBS_RAW
)
, t2 as (
select 
     mrg_log_file_name
   -- , mrg_log_file_full_path
   , mrg_log_table_detail
   , '/opt/sas/config_dmc/'||substr(mrg_log_file_name, 1, 5)||'SASApp/SASEnvironment/SASCode/Jobs/'||substr(mrg_log_file_name, 6) as mrg_code_file_full_path 
from t1 
)
, t3 as (
select 
     MRG_LOG_FILE_NAME as merge_log_file_name 
   , 'MERGE' as MERGE_OPERATION_TYPE
   , 'By SAS code' as MERGE_IDENTIFIED_SOURCE
   , upper(replace(replace(mrg_log_table_detail, 'MERGE INTO', ''), ' ', '')) as MERGE_OBJECT_NAME
from t2   
)
select 
     merge_log_file_name
   , max(MERGE_OPERATION_TYPE) as MERGE_OPERATION_TYPE
   -- , merge_object_name 
   ,  'MERGE object list:'||chr(10)||listagg(MERGE_OBJECT_NAME, ';') within group(order by MERGE_OBJECT_NAME) as MERGE_OBJECT_NAMES
from t3
group by merge_log_file_name
;

select * from JIANJUNDAI_SAS_MERGE_JOBS_RAW;

select * from JIANJUNDAI_SAS_MERGE_JOBS;

-- Load insert 
drop table JIANJUNDAI_SAS_INSERT_JOBS_RAW;
create table JIANJUNDAI_SAS_INSERT_JOBS_RAW
( 
  LOG_FILE_NAME varchar2(4000),
  LOG_TABLE_DETAIL varchar2(4000)
);

drop table JIANJUNDAI_SAS_INSERT_JOBS;
create table JIANJUNDAI_SAS_INSERT_JOBS as 
with t1 as (
select distinct 
     log_file_name
   , log_table_detail
from JIANJUNDAI_SAS_INSERT_JOBS_RAW
)
, t2 as (
select 
     log_file_name as insert_log_file_name 
   , 'INSERT' as INSERT_OPERATION_TYPE
   , 'By SAS code' as INSERT_IDENTIFIED_SOURCE
   , upper(substr(log_table_detail, instr(log_table_detail, 'into ', 1, 1)+ 5)) as INSERT_OBJECT_NAME
from t1   
)
select 
     insert_log_file_name
   , max(INSERT_OPERATION_TYPE)  as INSERT_OPERATION_TYPE
   , 'INSERT object list:'||chr(10)||listagg(INSERT_OBJECT_NAME, ';') within group(order by INSERT_OBJECT_NAME) as INSERT_OBJECT_NAMES
from t2 
group by insert_log_file_name
;

-- Load execute 
drop table JIANJUNDAI_SAS_EXEPRC_JOBS_RAW;
create table JIANJUNDAI_SAS_EXEPRC_JOBS_RAW
( 
  LOG_FILE_NAME varchar2(4000),
  -- LOG_FILE_FULL_PATH varchar2(4000),
  LOG_TABLE_DETAIL varchar2(4000)
);

drop table JIANJUNDAI_SAS_EXEPRC_JOBS;
create table JIANJUNDAI_SAS_EXEPRC_JOBS as 
with t1 as (
select distinct 
     log_file_name
   , log_table_detail
from JIANJUNDAI_SAS_EXEPRC_JOBS_RAW
)
, t2 as (
select 
     log_file_name   as exeprc_log_file_name
   , 'EXEPRC' as EXEPRC_OPERATION_TYPE
   , 'By SAS code' as EXEPRC_IDENTIFIED_SOURCE
   , ltrim(upper(log_table_detail)) as EXEPRC_OBJECT_NAME
from t1 
)
select 
     exeprc_log_file_name
   , max(EXEPRC_OPERATION_TYPE)  as EXEPRC_OPERATION_TYPE
   , 'ExecProc list:'||chr(10)||listagg(EXEPRC_OBJECT_NAME, ';') within group(order by EXEPRC_OBJECT_NAME) as EXEPRC_OBJECT_NAMES
from t2 
group by exeprc_log_file_name
;

drop table JIANJUNDAI_SAS_JOB_OPERATIONS;
create table JIANJUNDAI_SAS_JOB_OPERATIONS as 
with t_sas_files as (
select distinct exeprc_log_file_name as sas_file_name from JIANJUNDAI_SAS_EXEPRC_JOBS union
select distinct merge_log_file_name as sas_file_name from JIANJUNDAI_SAS_MERGE_JOBS union
select distinct insert_log_file_name as sas_file_name from JIANJUNDAI_SAS_INSERT_JOBS 
)
, t_info_1 as (
select 
  sas_file_name
, 'By SAS code' as IDENTIFIED_SOURCE
, ins.insert_object_names||chr(10)||chr(13)||
  mrg.merge_object_names||chr(10)||chr(13)||
  epr.exeprc_object_names as table_operation_list 
, ins.insert_operation_type 
, ins.insert_object_names
, mrg.merge_operation_type 
, mrg.MERGE_OBJECT_NAMEs
, epr.exeprc_operation_type 
, epr.exeprc_object_names
from t_sas_files   sas
left outer join JIANJUNDAI_SAS_INSERT_JOBS    ins on sas.sas_file_name = ins.insert_log_file_name
left outer join JIANJUNDAI_SAS_MERGE_JOBS     mrg on sas.sas_file_name = mrg.merge_log_file_name
left outer join JIANJUNDAI_SAS_EXEPRC_JOBS    epr on sas.sas_file_name = epr.exeprc_log_file_name
)
select * 
from t_info_1 
;


select * 
from JIANJUNDAI_SAS_JOB_OPERATIONS
;


drop table jianjundai_lsf_sas_final_comb ;
create table jianjundai_lsf_sas_final_comb as 
select 
    logs.*
    , codes.table_operation_list 
from jianjundai_lsf_sas_log_allinfo                        logs 
left outer join JIANJUNDAI_SAS_JOB_OPERATIONS              codes
on replace(replace(logs.lsf_sas_code_name, '/opt/sas/config_dmc/', ''), 'SASApp/SASEnvironment/SASCode/Jobs/', '') = codes.sas_file_name
--where codes.identified_source is null
--and lsf_job_name not in ('START_FLOW', 'END_FLOW')
;



select min(job_start_date), max(job_start_date)
from jianjundai_lsf_sas_log_allinfo;

select min(job_start_date), max(job_start_date)
from jianjundai_lsf_sas_log_detail;

-- Document query 
with lsf_ctas_insert_cnt as (
select distinct job_start_date, lsf_process_flow_name, sum(total_ctas_insert_rcdcnt) over(partition by job_start_date, lsf_process_flow_name) as total_ctas_insert_record_cnt
from jianjundai_lsf_sas_final_comb
where job_start_date = to_date('06-Feb-2024')
)
, t_main as (
select distinct 
   lsf_process_flow_name
   , max(job_start_date) over(partition by lsf_process_flow_name) as latest_run_date 
   , count(distinct lsf_id) over(partition by lsf_process_flow_name) as total_run_times
from jianjundai_lsf_log_detail
where job_start_date between to_date('01-Jul-2023', 'DD-Mon-YYYY') and to_date('31-Jan-2024', 'DD-Mon-YYYY') 
and lsf_process_flow_name is not null
order by 1 
)
select 
    t_main.*
    , lsf_ctas_insert_cnt.total_ctas_insert_record_cnt
from t_main
left outer join lsf_ctas_insert_cnt
on t_main.lsf_process_flow_name = lsf_ctas_insert_cnt.lsf_process_flow_name
;

select * 
from jianjundai_lsf_log_detail
where lsf_process_flow_name = 'DATAMART_ECL_CME'
;


-- Understand IBM Process Flow run history (see if it is able to work out how it is scheduled)
-- -- Understand how many Process Flow runs everyday between 01-Jul-2023 and 19-Feb-2024 
with t1 as (
select 
     job_start_date
   , to_char(job_start_date, 'Dy') as job_start_dow
   , lsf_process_flow_name          
   , count(*)   as lsf_process_flow_jobcount
from jianjundai_lsf_log_detail
where job_start_date between to_date('01-NOV-2023', 'DD-Mon-YYYY') and to_date('31-Mar-2024', 'DD-Mon-YYYY')
group by 
     job_start_date
   , lsf_process_flow_name
order by 1, 2 
)
select 
     job_start_date as ibm_process_flow_run_date
  ,  job_start_dow  as ibm_process_flow_run_date_dow
  ,  count(*) as number_of_process_flows_run
from t1 
group by 
     job_start_date
  ,  job_start_dow
order by 1 
;

-- -- Understand what process flows runs everyday and its job count between 01-Jul-2023 and 19-Feb-2024 
select 
     job_start_date                as ibm_process_flow_run_date
   , min(job_start_time)           as ibm_process_flow_run_stime
   , to_char(job_start_date, 'Dy') as ibm_process_flow_run_date_dow
   , lsf_process_flow_name         as ibm_process_flow_name 
   , count(*)                      as sas_process_flow_job_cnt 
   , trunc((max(job_start_time) - min(job_start_time)) * 24 * 60, 2) as sas_process_flow_duration_mins
from jianjundai_lsf_log_detail
where job_start_date between to_date('01-Feb-2024', 'DD-Mon-YYYY') and to_date('01-Feb-2024', 'DD-Mon-YYYY')
group by 
     job_start_date
   , lsf_process_flow_name
order by 1, 2
;

select 
     job_start_date
   , min(job_start_time)
   , lsf_id 
   , lsf_process_flow_name
--   , lsf_subprocess_name
--   , lsf_log_file_path_name
   , count(*)
from jianjundai_lsf_log_detail
group by 
     job_start_date
--   , job_start_time
   , lsf_id 
   , lsf_process_flow_name
--   , lsf_subprocess_name
--   , lsf_log_file_path_name
order by job_start_date, lsf_id 
;
-- Understand LSF and SAS job history, with each job Insert table names and counts and Merge table name 

select count(*)
, case when rcdcnt_file_name is null then 0 else 1 end as rcdcnt_file_name_flg 
, case when mrg_log_file_name is null then 0 else 1 end as mrg_log_file_name_flg
from JIANJUNDAI_LSF_SAS_LOG_ALLINFO
where job_start_date = to_date('06-Feb-2024', 'DD-Mon-YYYY') 
-- and lsf_sas_code_name like '%BIA%'
-- and rcdcnt_file_name is null 
-- order by lsf_id, job_start_time 
group by case when rcdcnt_file_name is null then 0 else 1 end 
, case when mrg_log_file_name is null then 0 else 1 end 
;

select distinct lsf_sas_code_name 
from JIANJUNDAI_LSF_SAS_LOG_ALLINFO
where job_start_date = to_date('06-Feb-2024', 'DD-Mon-YYYY') 
and case when rcdcnt_file_name is null then 0 else 1 end = 0 
and case when mrg_log_file_name is null then 0 else 1 end = 0 
and lsf_sas_code_name not in ('/opt/sas/config_dmc/Lev1/SASApp/SASEnvironment/SASCode/Jobs/START_FLOW.sas'
, '/opt/sas/config_dmc/Lev1/SASApp/SASEnvironment/SASCode/Jobs/END_FLOW.sas'
, '/opt/sas/config_dmc/Lev1/SASApp/SASEnvironment/SASCode/Jobs/DW_OTM_GRANTS.sas'
)
order by lsf_sas_code_name
;

select * 
from JIANJUNDAI_LSF_SAS_LOG_ALLINFO
where job_start_date = to_date('06-Feb-2024', 'DD-Mon-YYYY') 
;

-- List of IBM_PROCESS Flows get executed since 01-Jul-2023 and its latest execution date
-- list each of the process flow, what sas jobs and how its execution stats. 


-- Generate detail records for document
drop table jianjundai_document_detail;
create table jianjundai_document_detail as 
with lsf_ctas_insert_cnt as (
select 
   lsf_sas_code_name, 
   job_start_time, 
   cpu_time_duration, 
   real_time_duration, 
   total_ctas_insert_rcdcnt, 
   row_number() over(partition by lsf_sas_code_name order by job_start_time asc) as row_num_ 
from jianjundai_lsf_sas_final_comb
where job_start_date = to_date('06-Feb-2024')
and lsf_sas_code_name not in ('/opt/sas/config_dmc/Lev1/SASApp/SASEnvironment/SASCode/Jobs/START_FLOW.sas', '/opt/sas/config_dmc/Lev1/SASApp/SASEnvironment/SASCode/Jobs/END_FLOW.sas')
)
, t_output as ( 
select distinct 
   final_comb.lsf_process_flow_name
   , final_comb.lsf_subprocess_name
   , final_comb.lsf_job_name
   , final_comb.lsf_sas_code_name
   , final_comb.lsf_log_file_path_name 
   , final_comb.table_operation_list 
   , lsf_ctas_insert_cnt.job_start_time
   , lsf_ctas_insert_cnt.cpu_time_duration
   , lsf_ctas_insert_cnt.real_time_duration
   , lsf_ctas_insert_cnt.total_ctas_insert_rcdcnt
from jianjundai_lsf_sas_final_comb        final_comb
left outer join lsf_ctas_insert_cnt
on    final_comb.lsf_sas_code_name = lsf_ctas_insert_cnt.lsf_sas_code_name
and   lsf_ctas_insert_cnt.row_num_ = 1 
)
select 
     lsf_process_flow_name
   , lsf_subprocess_name
   , lsf_job_name
   , lsf_sas_code_name
   , lsf_log_file_path_name
   , table_operation_list
   , job_start_time
   , cpu_time_duration
   , real_time_duration
   , total_ctas_insert_rcdcnt
from t_output 
order by lsf_process_flow_name, job_start_time
;

-- Generate document detail section 

select lsf_process_flow_name
, listagg(table_operation_list) within group(order by job_start_time) as all_tables
from jianjundai_document_detail
group by lsf_process_flow_name
order by 1 
;
BIA_DATA.IPK_SOURCE_COUNTRY_LEVEL; BIA_DATA.IPK_SOURCE_MONTHLY_LEVEL; BIA_DATA.IMC_INBOUND_INTERCHANGE_IPK; BIA_DATA.IMC_UPU_INBOUND_SUMMARY;



select * from jianjundai_document_detail
