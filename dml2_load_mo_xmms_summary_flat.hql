set mapreduce.reduce.java.opts=-Xmx800m;
set mapreduce.map.java.opts=-Xmx800m;
set mapred.child.java.opts=-XX:-UseGCOverheadLimit;
set hbase.scan.cache=10000;
set hbase.client.scanner.cache=10000;
use cram;
add jar hdfs:///lib/brickhouse-0.6.0.jar;
add jar hdfs:///lib/HiveUtils-0.0.2.jar;
CREATE TEMPORARY FUNCTION collect AS 'brickhouse.udf.collect.CollectUDAF';
INSERT INTO TABLE mo_xmms_summary_flat
select
det.record_id,
hdd_date,
media_date,
load_date,
model_no,
head_vendor_id,
family_identifier,
product,
site_name,
hdd_serial_number,
wafer_id,
part_number,
media_pcn,
media_sputterline,
media_vendor,
head_id,
capacity,
eval_production,
test_type,
test_type_description,
ast_revision,
head_dcm,
media_dcm,
preamp_dcm,
mba_dcm,
act_dcm,
susp_dcm,
prime_rework_retest,
test_phase,
format_status,
eval_code,
substrate_code,
substrate,
head_vendor, 
hstack_vendor,
wafer,
media_tracecode,
hdd_test_run_start_date_time,
fiscal_week_id,
month_name,
fiscal_year,
fiscal_quarter_id,
collect(param_name, det.param_value) as details
from mo.xmms_details det
join mo.xmms_parameter param on param.param_id = det.param_id
inner join mo.xmms_header head on head.record_id = det.record_id
join mo.wd_fiscal_calendar cal on cal.calendar_date = head.hdd_date
where det.ts = ${hivevar:partition}
and param_value is not null
group by det.record_id, hdd_date, media_date, load_date, model_no, head_vendor_id, family_identifier, product,site_name, hdd_serial_number, wafer_id, part_number, media_pcn, media_sputterline, media_vendor, head_id, capacity, eval_production, test_type, test_type_description, ast_revision, head_dcm, media_dcm, preamp_dcm, mba_dcm, act_dcm, susp_dcm, prime_rework_retest, test_phase, format_status, eval_code, substrate_code, substrate, head_vendor,  hstack_vendor, wafer, media_tracecode, hdd_test_run_start_date_time, fiscal_week_id, month_name, fiscal_year, fiscal_quarter_id;
