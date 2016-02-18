use cram;
add jar hdfs:///lib/brickhouse-0.7.1-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION collect AS 'brickhouse.udf.collect.CollectUDAF';
INSERT INTO TABLE xmms_denorm_pg
select
denorm.record_id,
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
fiscal_week,
month_name,
fiscal_year,
fiscal_quarter,
defects,
incoming_head,
interface,
magnetic
from
    (select * from mo.xmms_header head
    join mo.wd_fiscal_calendar cal
    on head.hdd_date = cal.calendar_date) denorm
left outer join
    (select det.record_id, collect(param_name,cast(det.param_value as string)) as defects
    from mo.xmms_details det
    join mo.xmms_parameter param
    on det.param_id = param.param_id
    where param_value is not null
    and lower(param.param_group_nm) = 'defects'
    group by det.record_id) def
on denorm.record_id = def.record_id
left outer join
    (select det.record_id, collect(param_name,cast(det.param_value as string)) as interface
    from mo.xmms_details det
    join mo.xmms_parameter param
    on det.param_id = param.param_id
    where param_value is not null
    and lower(param.param_group_nm) = 'interface'
    group by det.record_id) inter
on denorm.record_id = inter.record_id
left outer join
    (select det.record_id, collect(param_name,cast(det.param_value as string)) as incoming_head
    from mo.xmms_details det
    join mo.xmms_parameter param
    on det.param_id = param.param_id
    where param_value is not null
    and lower(param.param_group_nm) = 'incoming head'
    group by det.record_id) incom
on denorm.record_id = incom.record_id
left outer join
    (select det.record_id, collect(param_name,cast(det.param_value as string)) as magnetic
    from mo.xmms_details det
    join mo.xmms_parameter param
    on det.param_id = param.param_id
    where param_value is not null
    and lower(param.param_group_nm) = 'magnetic'
    group by det.record_id) mag
on denorm.record_id = mag.record_id;
