with active as (
    select * from {{ref('int1_active_subs')}}
),

Bounced as (
    select * from {{ref('int2_bounced_subs')}}
),

Deleted as (
    select * from {{ref('int3_deleted_subs')}}
),

Unsubs as (
    select * from {{ref('int4_unsubs')}}
),

all_subs as ( 
    select 
        Growth_Bucket,
        Incentivization, 
        coalesce(active.Active_Volume, 0) as Active_Volume,
        active.Active_Unique_Open_Rate,
        active.Active_Click_Rate,
        active.Active_Total_CTOR,
        active.Active_Partner_Click_Rate,
        active.Active_Partner_Total_CTOR,
        coalesce(Unsubs.Unsubs_Volume, 0) as Unsubs_Volume,
        Unsubs.Unsubs_Unique_Open_Rate,
        Unsubs.Unsubs_Click_Rate,
        Unsubs.Unsubs_Total_CTOR,
        Unsubs.Unsubs_Partner_Click_Rate,
        Unsubs.Unsubs_Partner_Total_CTOR,
        coalesce(Bounced.bounced_Volume, 0) as Bounced_Volume,
        Bounced.bounced_Unique_Open_Rate,
        Bounced.bounced_Click_Rate,
        Bounced.bounced_Total_CTOR,
        Bounced.bounced_Partner_Click_Rate,
        Bounced.bounced_Partner_Total_CTOR,
        coalesce(Deleted.deleted_Volume, 0) as Deleted_Volume,
        Deleted.deleted_Unique_Open_Rate,
        Deleted.deleted_Click_Rate,
        Deleted.deleted_Total_CTOR,
        Deleted.deleted_Partner_Click_Rate,
        Deleted.deleted_Partner_Total_CTOR
    from active 
    LEFT JOIN Unsubs using (Growth_Bucket, Incentivization)
    LEFT JOIN deleted using (Growth_Bucket, Incentivization)
    LEFT JOIN bounced using (Growth_Bucket, Incentivization)
)

select * from all_subs
order by 1
