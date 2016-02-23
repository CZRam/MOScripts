use cram;
INSERT INTO TABLE mo_xmms_summary_expanded
select
named_struct('record_id',denorm.record_id,'param_group_nm', param_group_nm,'param_id',param_id),
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
param_group_nm,
param_id,
param_value
from
    (select det.record_id, det.param_id, param.param_group_nm, det.param_value
    from mo.xmms_details det
    join mo.xmms_parameter param
    on det.param_id = param.param_id
    where det.param_value is not null) dets
join
    (select * from mo.xmms_header head
    join mo.wd_fiscal_calendar cal
    on head.hdd_date = cal.calendar_date) denorm
on denorm.record_id = dets.record_id
group by denorm.record_id, param_group_nm, hdd_date, media_date, load_date, model_no, head_vendor_id, family_identifier, product,site_name, hdd_serial_number, wafer_id, part_number, media_pcn, media_sputterline, media_vendor, head_id, capacity, eval_production, test_type, test_type_description, ast_revision, head_dcm, media_dcm, preamp_dcm, mba_dcm, act_dcm, susp_dcm, prime_rework_retest, test_phase, format_status, eval_code, substrate_code, substrate, head_vendor,  hstack_vendor, wafer, media_tracecode, hdd_test_run_start_date_time, fiscal_week, month_name, fiscal_year, fiscal_quarter, param_id, param_value;
