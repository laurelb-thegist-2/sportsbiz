-- pulls from user queries in the intermediate -> 04_user_data folders

with OPEN_SEND_CLICK_SUMMARY as (
    select * from {{ref('sb_int4_sends_opens_clicks_by_user')}}
),

SUBSCRIBERS as (
    select * from {{ref('sb_int4_final_subscribers')}}
),

Total_subs as (
SELECT 
    coalesce(SUBSCRIBERS.Growth_Bucket, 'Organic/Unknown') as Growth_Bucket,
    coalesce(SUBSCRIBERS.Incentivization, 'Unincentivized') as Incentivization,
    count(Email) Total_Volume,
    sum(CASE WHEN unique_opens > 0 THEN 1 ELSE 0 END) Openers,
    sum(CASE WHEN total_clicks > 0 THEN 1 ELSE 0 END) Clickers,
    sum(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) Active_Volume,
    sum(CASE WHEN status = 'Active' THEN unique_opens END)/sum(CASE WHEN status = 'Active' THEN delivered END) Active_UOR,
    sum(CASE WHEN status = 'Active' THEN total_clicks END)/sum(CASE WHEN status = 'Active' THEN delivered END) Active_CTR,
    sum(CASE WHEN status = 'Active' THEN total_partner_clicks END)/sum(CASE WHEN status = 'Active' THEN delivered END) Active_Partner_CTR,
    sum(CASE WHEN status = 'Unsubscribed' THEN 1 ELSE 0 END) Unsubscribed_Volume,
    sum(CASE WHEN status = 'Deleted' THEN 1 ELSE 0 END) Deleted_Volume,
    sum(CASE WHEN status = 'Bounced' THEN 1 ELSE 0 END) Bounced_Volume
FROM OPEN_SEND_CLICK_SUMMARY
LEFT JOIN SUBSCRIBERS using (EMAIL)
WHERE FIRST_SEND > '2021-12-31'
Group by 1,2
)

select *
from Total_subs
ORDER BY 1