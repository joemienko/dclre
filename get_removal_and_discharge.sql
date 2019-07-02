with removal_counts as
(
select distinct 
	month_name
	,calendar_month
	,jt.county_desc
	,fips
	,count(distinct id_removal_episode_fact) removal_count
from ##join_table jt
	left join ##court_episodes ce on latest_removal_dt between first_of_month and lastofmonth
		and jt.county_desc = ce.county_desc 
where calendar_date between '2005-01-01' and '2019-04-01'
group by 
	month_name
	,calendar_month
	,jt.county_desc
	,fips
), discharge_counts as
(
select distinct 
	month_name
	,calendar_month
	,jt.county_desc
	,fips
	,count(distinct id_removal_episode_fact) discharge_count
from ##join_table jt
	left join ##court_episodes ce on discharge_from_pilot between first_of_month and lastofmonth
		and jt.county_desc = ce.county_desc 
where calendar_date between '2005-01-01' and '2019-04-01'
group by 
	month_name
	,calendar_month
	,jt.county_desc
	,fips
)

select 
	rc.month_name
	,rc.calendar_month
	,rc.county_desc
	,rc.removal_count
	,rc.fips
	,dc.discharge_count
from removal_counts rc
  left join discharge_counts dc
	  on rc.calendar_month = dc.calendar_month
	    and rc.county_desc = dc.county_desc;