
  create or replace   view AIRBNB.dev.src_hosts
  
   as (
    with raw_hosts as (select * from AIRBNB.RAW.raw_hosts)
select id as host_id, name as host_name, is_superhost, created_at, updated_at
from raw_hosts
  );

