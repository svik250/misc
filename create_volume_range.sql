CREATE TABLE volume_range AS

with all_volume as (select symbol, cast(datetime as time) as time, avg(volume) as average_volume from candle60 group by  symbol, cast(datetime as time)),

range_volume as 
(select symbol,
case when time >= '09:15:00' and time <= '09:21:00' then '09:15-09:21' 
when time >= '09:22:00' and time <= '09:45:00' then '09:22-09:45' 
when time >= '09:46:00' and time <= '10:15:00' then '09:46-10:15' 
when time >= '10:16:00' and time <= '11:15:00' then '10:16-11:15' 
when time >= '11:16:00' and time <= '13:45:00' then '11:16-13:45' 
when time >= '13:46:00' and time <= '14:55:00' then '13:46-14:50' 
when time >= '14:56:00' and time <= '15:30:00' then '14:56-15:30' 
else 'invalid' end as "time_range", average_volume
from all_volume)

select symbol, time_range, avg(average_volume) as average_volume from range_volume group by symbol, time_range;