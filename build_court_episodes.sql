if object_id('tempdb..##court_episodes') is not null
    drop table ##court_episodes;

select distinct 
  rp.latest_removal_dt
  ,id_removal_episode_fact
  ,rp.child
  ,rp.removal_county_cd
  ,rlc.county_desc
  ,rp.discharge_dt
  ,rp.id_case
  ,case
    when lf_dt is not null
    then lf_dt
  else discharge_dt
end discharge_from_pilot
into ##court_episodes
from base.rptPlacement rp
  left join dbo.legal_fact lf
    on lf.id_case = rp.id_case
  left join dbo.legal_jurisdiction_dim ljd 
    on lf.id_legal_jurisdiction_dim = ljd.id_legal_jurisdiction_dim
  join dbo.ref_lookup_county rlc
	on rp.removal_county_cd = rlc.county_cd
where 
--(rp.tx_county in ('Grant', 'Lewis', 'Whatcom') 
  --  or ljd.cd_jurisdiction = 9)
  removal_county_cd != 99
  and dbo.fnc_datediff_yrs(birthdate, removal_dt) < 18
  and (tx_lgl_stat in 
  ('Closed - Adoption'
  ,'Closed - Dependency Guardianship No Supv'
  ,'Closed - Superior Court Guardianship'
  ,'Closed - Title 13 Guardianship'
  ,'Closed -Non-parental (3rd party) custody'
  ,'Dependency Guardianship'
  ,'Dependent'
  ,'Dependent - Legally Free'
  ,'EFC Dependent'
  ,'Non-parental (3rd Party) custody'
  ,'Parental Custody -Continued Court Action'
  ,'Protective Custody'
  ,'Shelter Care'
  ,'Superior Court Guardianship')
  or tx_dsch_rsn = 'Returned to Custody of Parents - Dependency Dismissed'
  or fl_dep_exist = 1);