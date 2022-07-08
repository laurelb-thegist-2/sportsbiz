with user_data as (
    select * from {{ref('sb_fnl_user_data_summary')}}
),

rfm_analysis as (
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
        MOST_RECENT_CLICK >= dateadd(day, -14, GETDATE()) AND Click_Rate <= 10 AND Click_Rate >= 0 -- recency = less than or equal to 2 weeks ago / frequency = more than no clicks but less than 10 per newsletter sent
    THEN 'Good'
    WHEN
        MOST_RECENT_CLICK < dateadd(day, -14, GETDATE()) AND Click_Rate <= 10 AND Click_Rate > 0 -- recency = more than 2 weeks ago / frequency = more than no clicks but less than 10 per newsletter sent
    THEN 'Medium'
    WHEN
        Click_Rate = 0 or Click_Rate > 10 -- no clicks or more than 10 per newsletter
    THEN 'Bad'
    ELSE 'Bad' END as Recency_Frequency
from user_data
)

select * from rfm_analysis