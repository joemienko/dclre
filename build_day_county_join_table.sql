if object_id('tempdb..##join_table') is not null
    drop table ##join_table;

select distinct 
	calendar_date
	,month_name
	,month calendar_month
	,first_of_month
	,lastofmonth
	,county_desc 
	,countyfips fips
into ##join_table
from calendar_dim cd, dbo.ref_lookup_county rlc
where county_cd between 1 and 39
	and calendar_date between '2005-01-01' and '2019-04-01'
