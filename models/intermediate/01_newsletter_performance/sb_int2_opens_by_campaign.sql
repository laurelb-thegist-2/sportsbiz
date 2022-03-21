with OPENS as (
    select * from {{ref('sb_stg_campaign_opens')}}
),

CAMPAIGN_DETAILS as (
    select * from {{ref('sb_stg_campaign_details')}}
),

opens_by_campaign as (
    select
        Campaign_Details.NAME,
        Campaign_Details.CAMPAIGN_DATE,
        OPENS.*
    from CAMPAIGN_DETAILS
LEFT JOIN OPENS using (Campaign_ID)
)

select * from opens_by_campaign