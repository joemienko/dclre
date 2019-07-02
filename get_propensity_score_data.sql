with treatment_episodes as
(
select  
	id_removal_episode_fact
from ##court_episodes
where latest_removal_dt >= '2017-09-01'
	and county_desc in ('Lewis', 'Grant')
)
select
	child id_prsn_child
	,rp.id_removal_episode_fact
	,iif(rp.id_removal_episode_fact in (select * from treatment_episodes), 1, 0) tx
	,tx_gndr
	,tx_braam_race
	,age_at_removal_mos
	,child_eps_rank
	,child_cnt_episodes
	,nbr_ooh_events
	,rlp1.tx_plcm_setng tx_plcm_setng_first
	,rlp2.tx_plcm_setng tx_plcm_setng_longest
	,long_cd_plcm_setng
	,fl_abandonment
	,fl_caretaker_inability_cope
	,fl_child_abuse_alcohol
	,fl_child_abuses_drug
	,fl_child_behavior_problems
	,fl_inadequate_housng
	,fl_neglect
	,fl_parent_abuse_alcohol
	,fl_parent_death
	,fl_parent_drug_abuse
	,fl_parent_incarceration
	,tx_region	
from base.rptPlacement rp
  left join treatment_episodes te
    on rp.id_removal_episode_fact = te.id_removal_episode_fact
  join dbo.ref_lookup_plcmnt rlp1
	  on rp.init_cd_plcm_setng = rlp1.cd_plcm_setng
  join dbo.ref_lookup_plcmnt rlp2
  	on rp.long_cd_plcm_setng = rlp2.cd_plcm_setng
where rp.removal > '2010-01-01'
  and tx_region not in ('CONVERSION', 'Region 7')
  and tx_gndr in ('Male', 'Female');