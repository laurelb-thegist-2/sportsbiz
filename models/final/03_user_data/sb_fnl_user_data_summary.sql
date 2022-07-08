with OPEN_SEND_CLICK_SUMMARY as (
    select * from {{ref('sb_int4_sends_opens_clicks_by_user')}}
),

SUBSCRIBERS as (
    select * from {{ref('sb_int4_final_subscribers')}}
),

user_data_by_growth_channel as (
SELECT 
    SUBSCRIBERS.Growth_Channel,
    SUBSCRIBERS.Growth_Int_Bucket,
    SUBSCRIBERS.Growth_Bucket,
    FIRST_SEND,
    MOST_RECENT_SEND,
    MOST_RECENT_OPEN,
    MOST_RECENT_CLICK,
    SUBSCRIBERS.Email,
    SUBSCRIBERS.Status,
    sum(delivered) AS DELIVERED,
    sum(unique_opens) as UNIQUE_OPENS,
    sum(total_opens) as TOTAL_OPENS,
    sum(total_clicks) as TOTAL_CLICKS,
    case when sum(delivered) > 0 then sum(unique_opens)/sum(delivered) else 0 end as UNIQUE_OPEN_RATE,
    case when sum(delivered) > 0 then sum(total_clicks)/sum(delivered) else 0 end as CLICK_RATE,
    case when sum(total_opens) > 0 then sum(total_clicks)/sum(total_opens) else 0 end as TOTAL_CTOR
FROM OPEN_SEND_CLICK_SUMMARY
LEFT JOIN SUBSCRIBERS using (EMAIL)
Group by 1,2,3,4,5,6,7,8,9
)

select * from user_data_by_growth_channel
ORDER BY 1,2,3,4,5,6
limit 1000000