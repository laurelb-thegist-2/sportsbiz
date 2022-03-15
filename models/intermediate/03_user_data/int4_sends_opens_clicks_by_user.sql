with SEND_SUMMARY as (
    select * from {{ref('int1_sends_by_user')}}
),

OPEN_SUMMARY as (
    select * from {{ref('int2_opens_by_user')}}
),

CLICK_SUMMARY as (
    select * from {{ref('int3_clicks_by_user')}}
),

OPEN_SEND_CLICK_SUMMARY as 
(
    SELECT 
        SEND_SUMMARY.EMAIL,
        SEND_SUMMARY.FIRST_SEND,
        SEND_SUMMARY.MOST_RECENT_SEND,
        SEND_SUMMARY.TOTAL_SENDS,
        SEND_SUMMARY.Total_Bounced,
        SEND_SUMMARY.TOTAL_SENDS - SEND_SUMMARY.Total_Bounced as Delivered,
        CASE WHEN FIRST_OPEN IS NULL THEN NULL ELSE FIRST_OPEN END FIRST_OPEN,
        CASE WHEN MOST_RECENT_OPEN IS NULL THEN NULL ELSE MOST_RECENT_OPEN END MOST_RECENT_OPEN,
        CASE WHEN UNIQUE_OPENS IS NULL THEN 0 ELSE UNIQUE_OPENS END UNIQUE_OPENS,
        CASE WHEN TOTAL_OPENS IS NULL THEN 0 ELSE TOTAL_OPENS END TOTAL_OPENS,
        CLICK_SUMMARY.CAMPAIGNS_CLICKED,
        CLICK_SUMMARY.TOTAL_CLICKS,
        CLICK_SUMMARY.TOTAL_PARTNER_CLICKS,
        CLICK_SUMMARY.UNIQUE_PARTNER_CLICKS
    FROM SEND_SUMMARY 
    LEFT JOIN OPEN_SUMMARY using (EMAIL) 
    LEFT JOIN CLICK_SUMMARY using (EMAIL)
)

select * from OPEN_SEND_CLICK_SUMMARY