CREATE TABLE IF NOT EXISTS details_${hivevar:group}_${hivevar:partition}(
record_id string,
param_group_nm string,
param_name string,
param_id int,
param_value double);
INSERT INTO details_${hivevar:group}_${hivevar:partition}
SELECT
	det.record_id,
    param_group_nm,
    param_name,
    det.param_id,
    param_value
from mo.xmms_details det
join mo.xmms_parameter param
on det.param_id = param.param_id
where ts = ${hivevar:partition}
and lower(param_group_nm) = '${hivevar:group}'
and param_value is not null;
