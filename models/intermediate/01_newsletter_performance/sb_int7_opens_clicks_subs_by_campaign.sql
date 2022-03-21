with opens as (
    select * from {{ref('sb_int5_opens_subs_by_campaign')}}
),

clicks as (
    select * from {{ref('sb_int6_clicks_subs_by_campaign')}}
),

opens_clicks as (
    select 
        opens.CAMPAIGN_ID,
        opens.NAME,
        opens.CAMPAIGN_DATE,
        opens.COUNTRY,
        opens.City,
        opens.Growth_Channel,
        opens.Growth_Bucket,
        opens.Incentivization,
        opens.Total_Opens,
        opens.Gmail_Total_Opens,
        opens.Non_Gmail_Total_Opens,
        opens.Unique_Opens,
        CASE WHEN clicks.Total_Clicks is NULL THEN 0 ELSE clicks.Total_Clicks END Total_Clicks,
        CASE WHEN clicks.Unique_Clicks is NULL THEN 0 ELSE clicks.Unique_Clicks END Unique_Clicks
    from opens
    FULL OUTER JOIN clicks 
    USING (Campaign_ID, Name, CAMPAIGN_DATE, COUNTRY, City, Growth_Channel, Growth_Bucket, Incentivization)
)

select * from opens_clicks
WHERE CAMPAIGN_DATE IS NOT NULL
ORDER BY CAMPAIGN_DATE DESC