with opens_subscribers as (
    select * from {{ref('int5_opens_subs_by_campaign')}}
),

clicks_subscribers as (
    select * from {{ref('int6_clicks_subs_by_campaign')}}
),

opens_clicks_subscribers as (
    select 
        opens_subscribers.CAMPAIGN_ID,
        opens_subscribers.NAME,
        opens_subscribers.CAMPAIGN_DATE,
        sum(opens_subscribers.Total_Opens) Total_Opens,
        sum(opens_subscribers.Gmail_Total_Opens) Gmail_Total_Opens,
        sum(opens_subscribers.Non_Gmail_Total_Opens) Non_Gmail_Total_Opens,
        sum(opens_subscribers.Unique_Opens) Unique_Opens,
        sum(clicks_subscribers.Total_Clicks) Total_Clicks,
        sum(clicks_subscribers.Unique_Clicks) Unique_Clicks
    from opens_subscribers
    LEFT JOIN clicks_subscribers 
    USING (Campaign_ID, Name, CAMPAIGN_DATE, COUNTRY, City, Growth_Channel, Growth_Bucket, Incentivization)
    GROUP BY 1, 2, 3
)

select * from opens_clicks_subscribers
WHERE CAMPAIGN_DATE IS NOT NULL
ORDER BY CAMPAIGN_DATE DESC