with EMAIL_SENDS as (
    select * from {{ref('stg_email_sends')}}
),

CAMPAIGN_DETAILS as (
    select * from {{ref('stg_campaign_details')}}
),

Campaign_Bounces as (
    select * from {{ref('stg_campaign_bounces')}}
),

sends as (
    select
        CAMPAIGN_DATE,
        Campaign_ID,
        NAME,
        EMAIL_SENDS.email,
        Campaign_Bounces.email Bounced_Emails
    from CAMPAIGN_DETAILS
LEFT JOIN EMAIL_SENDS using (Campaign_ID)
LEFT JOIN Campaign_Bounces using (Campaign_ID, Email)
)

select * from sends