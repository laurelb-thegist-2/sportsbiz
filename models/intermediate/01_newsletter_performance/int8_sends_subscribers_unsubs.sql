with sends_subscribers as (
    select * 
    from {{ref('int4_sends_subs_by_campaign')}}
),

CAMPAIGN_DETAILS as (
    select * from {{ref('stg_campaign_details')}}
),

sends_subscribers_unsubs as (
    select
        sends_subscribers.CAMPAIGN_ID, 
        sends_subscribers.Name,
        sends_subscribers.CAMPAIGN_DATE,
        CAMPAIGN_DETAILS.Total_Unsubscribes,
        sum(sends_subscribers.Total_Sends) Total_Sends,
        sum(sends_subscribers.Total_Bounced) Total_Bounced,
        sum(sends_subscribers.Delivered_Emails) Delivered_Emails,
        sum(sends_subscribers.Gmail_Total_Sends) Gmail_Total_Sends,
        sum(sends_subscribers.Non_Gmail_Total_Sends) Non_Gmail_Total_Sends
    from sends_subscribers
    LEFT JOIN CAMPAIGN_DETAILS using (CAMPAIGN_ID, Name, CAMPAIGN_DATE)
    GROUP BY 1, 2, 3, 4
)

select * from sends_subscribers_unsubs
where CAMPAIGN_DATE is not null
order by CAMPAIGN_DATE desc