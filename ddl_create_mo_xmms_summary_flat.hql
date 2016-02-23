use cram;
CREATE TABLE IF NOT EXISTS mo_xmms_summary_flat( 
record_id	bigint,
hdd_date	date,
media_date	date,
load_date	timestamp,
model_no	string,
head_vendor_id	string,
family_identifier	string,
product	string,
site_name	string,
hdd_serial_number	string,
wafer_id	string,
part_number	string,
media_pcn	string,
media_sputterline	string,
media_vendor	string,
head_id	string,
capacity	string,
eval_production	string,
test_type	string,
test_type_description	string,
ast_revision	string,
head_dcm	string,
media_dcm	string,
preamp_dcm	string,
mba_dcm	string,
act_dcm	string,
susp_dcm	string,
prime_rework_retest	string,
test_phase	string,
format_status	string,
eval_code	string,
substrate_code	string,
substrate	string,
head_vendor	string,
hstack_vendor	string,
wafer	string,
media_tracecode	string,
hdd_test_run_start_date_time	timestamp, 
fiscal_week	smallint,
month_name	string,
fiscal_year	int,
fiscal_quarter	smallint,
details map<string,double>)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,
header:hdd_date,
header:media_date,
header:load_date,
header:model_no,
header:head_vendor_id,
header:family_identifier,
header:product,
header:site_name,
header:hdd_serial_number,
header:wafer_id,
header:part_number,
header:media_pcn,
header:media_sputterline,
header:media_vendor,
header:head_id,
header:capacity,
header:eval_production,
header:test_type,
header:test_type_description,
header:ast_revision,
header:head_dcm,
header:media_dcm,
header:preamp_dcm,
header:mba_dcm,
header:act_dcm,
header:susp_dcm,
header:prime_rework_retest,
header:test_phase,
header:format_status,
header:eval_code,
header:substrate_code,
header:substrate,
header:head_vendor,
header:hstack_vendor,
header:wafer,
header:media_tracecode,
header:hdd_test_run_start_date_time,
header:fiscal_week,
header:month_name,
header:fiscal_year,
header:fiscal_quarter,
details:")
TBLPROPERTIES ("hbase.table.name" = "mo_xmms_summary_flat");