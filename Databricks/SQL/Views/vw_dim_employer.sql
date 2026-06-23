%sql


create or replace view {$Catalog}.viz_views.vw_dim_Employer as
(
  select
    EmployerKey,
    employer_name,
    employer_reviews,
    employer_website,
    country
  from
    {$Catalog}.presented_master.tb_dim_employer
)