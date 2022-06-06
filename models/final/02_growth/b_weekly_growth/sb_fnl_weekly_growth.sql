with OPEN_SEND_CLICK_SUMMARY as (
    select * from {{ref('sb_int4_sends_opens_clicks_by_user')}}
),

SUBSCRIBERS as (
    select * from {{ref('sb_int4_final_subscribers')}}
),

new_subs_by_growth_channel as (
SELECT 
    FIRST_SEND Date,
    SUBSCRIBERS.Growth_Channel Growth_Channel_DBT,
    subscribers.Growth_Int_Bucket,
    subscribers.Growth_Bucket,
    subscribers.Incentivization,
    subscribers.Status,
    coalesce(subscribers.Country, 'US') Country,
    coalesce(subscribers.Cities, 'None') Cities,
    SUBSCRIBERS.source_brand,
    SUBSCRIBERS.campaign_name,
    count(SUBSCRIBERS.EMAIL) as GROWTH
FROM OPEN_SEND_CLICK_SUMMARY
LEFT JOIN SUBSCRIBERS using (EMAIL)
WHERE OPEN_SEND_CLICK_SUMMARY.FIRST_SEND > dateadd(day, -8, GETDATE()) --Sunday
AND OPEN_SEND_CLICK_SUMMARY.FIRST_SEND < GETDATE() --Monday
Group by 1,2,3,4,5,6,7,8,9,10
ORDER BY 1 DESC
)

select * from new_subs_by_growth_channel
limit 10000