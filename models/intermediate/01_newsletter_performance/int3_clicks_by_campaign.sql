with CLICKS as (
    select * from {{ref('stg_campaign_clicks')}}
),

CAMPAIGN_DETAILS as (
    select * from {{ref('stg_campaign_details')}}
),

clicks_by_campaign as (
    select
        Campaign_Details.NAME,
        Campaign_Details.CAMPAIGN_DATE,
        CLICKS.*
    from CAMPAIGN_DETAILS
LEFT JOIN clicks using (Campaign_ID)
)

select * from clicks_by_campaign