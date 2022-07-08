with user_data as (
    select * from {{ref('sb_fnl_user_data_summary')}}
),

open_rfm_analysis as (
select
    Growth_Channel,
    Growth_Int_Bucket,
    Growth_Bucket,
    Email,
    Status,
    Delivered,
    MOST_RECENT_OPEN,
    MOST_RECENT_CLICK,
    Unique_Opens,
    UNIQUE_OPEN_RATE,
    Total_Clicks,
    CLICK_RATE,
    CASE
    WHEN 
        (MOST_RECENT_OPEN >= dateadd(day, -7, GETDATE()) AND UNIQUE_OPEN_RATE >= 0.3) -- recency = <=1 week / frequency = 30-90%+
        OR 
        (MOST_RECENT_OPEN < dateadd(day, -7, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE >= 0.6) -- recency = 2 weeks / frequency = 60-90%+
    THEN 'Good'
    WHEN
        (MOST_RECENT_OPEN < dateadd(day, -14, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -21, GETDATE()) AND UNIQUE_OPEN_RATE >= 0.3) -- recency = 3 weeks / frequency = 30-90%+
        OR 
        (MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE < 0.3 and UNIQUE_OPEN_RATE > 0) -- recency = <=2 weeks / frequency = 0-30%
        OR 
        (MOST_RECENT_OPEN < dateadd(day, -7, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE < 0.6 and UNIQUE_OPEN_RATE >= 0.3) -- recency = 2 weeks / frequency = 30-60%
    THEN 'Medium'
    WHEN
        (MOST_RECENT_OPEN < dateadd(day, -21, GETDATE()) or MOST_RECENT_OPEN is NULL) -- never opened or haven't opened in past three weeks
        OR 
        (UNIQUE_OPEN_RATE = 0) -- never opened
        OR 
        (MOST_RECENT_OPEN < dateadd(day, -14, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -21, GETDATE()) AND UNIQUE_OPEN_RATE < 0.3 and UNIQUE_OPEN_RATE > 0) -- recency = 3 weeks / frequency = 1-30%
    THEN 'Bad'
    ELSE 'Bad' END as Recency_Frequency
from user_data
)

select * from open_rfm_analysis
