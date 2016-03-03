set mapreduce.reduce.java.opts=-Xmx800m;
set mapreduce.map.java.opts=-Xmx800m;
set mapred.child.java.opts=-XX:-UseGCOverheadLimit;
set mapred.child.java.opts=-XX:+UseConcMarkSweepGC;
add jar hdfs:///lib/brickhouse-0.6.0.jar;
add jar hdfs:///lib/HiveUtils-0.0.2.jar;

CREATE TEMPORARY FUNCTION collect AS 'brickhouse.udf.collect.CollectUDAF';
INSERT INTO TABLE cram.mo_xmms_summary_group
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
fiscal_week_id,
month_name,
fiscal_year,
fiscal_quarter_id,
defects,
incoming_head,
interface,
magnetic
from
    (select * from mo.xmms_header head
    join mo.wd_fiscal_calendar cal
    on head.hdd_date = cal.calendar_date) denorm
left outer join
    (select record_id, collect(param_name,param_value) as defects
    from default.details_defects_${hivevar:partition}
    group by record_id) def
on denorm.record_id = def.record_id
left outer join
    (select record_id, collect(param_name,param_value) as interface
    from default.details_interface_${hivevar:partition}
    group by record_id) inter
on denorm.record_id = inter.record_id
left outer join
    (select record_id, collect(param_name,param_value) as incoming_head
    from default.details_incoming_head_${hivevar:partition}
    group by record_id) incom
on denorm.record_id = incom.record_id
left outer join
    (select record_id, collect(param_name,param_value) as magnetic
    from default.details_magnetic_${hivevar:partition}
    group by record_id) mag
on denorm.record_id = mag.record_id;

