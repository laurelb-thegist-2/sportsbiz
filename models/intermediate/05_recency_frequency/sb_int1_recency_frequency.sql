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
        (
            ( 
                (MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE < 0.3 and UNIQUE_OPEN_RATE > 0) -- MEDIUM
                OR
                (MOST_RECENT_OPEN < dateadd(day, -14, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -21, GETDATE()) AND UNIQUE_OPEN_RATE >= 0.3) -- MEDIUM
                OR
                (MOST_RECENT_OPEN < dateadd(day, -7, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE < 0.6 and UNIQUE_OPEN_RATE >= 0.3) -- MEDIUM
            ) 
        AND
                (MOST_RECENT_CLICK >= dateadd(day, -14, GETDATE()) AND Click_Rate <= 10 AND Click_Rate >= 0) -- GOOD
        ) 
    OR 
        (
            (
                (MOST_RECENT_OPEN >= dateadd(day, -7, GETDATE()) AND UNIQUE_OPEN_RATE >= 0.3) -- GOOD
                OR
                (MOST_RECENT_OPEN < dateadd(day, -7, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE >= 0.6) -- GOOD
            ) 
        AND 
                (MOST_RECENT_CLICK < dateadd(day, -14, GETDATE()) AND Click_Rate <= 10 AND Click_Rate > 0) -- MEDIUM
        ) 
    OR 
        (
            (
                (MOST_RECENT_OPEN >= dateadd(day, -7, GETDATE()) AND UNIQUE_OPEN_RATE >= 0.3) -- GOOD
                OR
                (MOST_RECENT_OPEN < dateadd(day, -7, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE >= 0.6) -- GOOD
            ) 
        AND 
                (MOST_RECENT_CLICK >= dateadd(day, -14, GETDATE()) AND Click_Rate <= 10 AND Click_Rate >= 0) -- GOOD
        ) 
    THEN 'Good'
    WHEN 
        (
            ( 
                (MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE < 0.3 and UNIQUE_OPEN_RATE > 0) -- MEDIUM
                OR
                (MOST_RECENT_OPEN < dateadd(day, -14, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -21, GETDATE()) AND UNIQUE_OPEN_RATE >= 0.3) -- MEDIUM
                OR
                (MOST_RECENT_OPEN < dateadd(day, -7, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE < 0.6 and UNIQUE_OPEN_RATE >= 0.3) -- MEDIUM
            ) 
        AND 
                (MOST_RECENT_CLICK < dateadd(day, -14, GETDATE()) AND Click_Rate <= 10 AND Click_Rate > 0) -- MEDIUM
        ) 
    OR 
        (
            (
                (MOST_RECENT_OPEN >= dateadd(day, -7, GETDATE()) AND UNIQUE_OPEN_RATE >= 0.3) -- GOOD
                OR
                (MOST_RECENT_OPEN < dateadd(day, -7, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE >= 0.6) -- GOOD
            ) 
        AND 
                (Click_Rate = 0 or Click_Rate > 10) -- BAD
        ) 
    THEN 'Medium'
    WHEN 
        (
            ( 
                (MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE < 0.3 and UNIQUE_OPEN_RATE > 0) -- MEDIUM
                OR
                (MOST_RECENT_OPEN < dateadd(day, -14, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -21, GETDATE()) AND UNIQUE_OPEN_RATE >= 0.3) -- MEDIUM
                OR
                (MOST_RECENT_OPEN < dateadd(day, -7, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE < 0.6 and UNIQUE_OPEN_RATE >= 0.3) -- MEDIUM
            ) 
        AND 
                (Click_Rate = 0 or Click_Rate > 10) -- BAD
        ) 
    OR 
        (
            (
                (UNIQUE_OPEN_RATE = 0) -- BAD
                OR
                (MOST_RECENT_OPEN < dateadd(day, -21, GETDATE()) or MOST_RECENT_OPEN is NULL) -- BAD
                OR
                (MOST_RECENT_OPEN < dateadd(day, -14, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -21, GETDATE()) AND UNIQUE_OPEN_RATE < 0.3 and UNIQUE_OPEN_RATE > 0) -- BAD
            ) 
        AND 
                (MOST_RECENT_CLICK < dateadd(day, -14, GETDATE()) AND Click_Rate <= 10 AND Click_Rate > 0) -- MEDIUM
        )
    OR 
        (
            (
                (UNIQUE_OPEN_RATE = 0) -- BAD
                OR
                (MOST_RECENT_OPEN < dateadd(day, -21, GETDATE()) or MOST_RECENT_OPEN is NULL) -- BAD
                OR
                (MOST_RECENT_OPEN < dateadd(day, -14, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -21, GETDATE()) AND UNIQUE_OPEN_RATE < 0.3 and UNIQUE_OPEN_RATE > 0) -- BAD
            ) 
        AND 
                (MOST_RECENT_CLICK >= dateadd(day, -14, GETDATE()) AND Click_Rate <= 10 AND Click_Rate >= 0) -- GOOD
        ) 
    OR 
        (
            (
                (UNIQUE_OPEN_RATE = 0) -- BAD
                OR
                (MOST_RECENT_OPEN < dateadd(day, -21, GETDATE()) or MOST_RECENT_OPEN is NULL) -- BAD
                OR
                (MOST_RECENT_OPEN < dateadd(day, -14, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -21, GETDATE()) AND UNIQUE_OPEN_RATE < 0.3 and UNIQUE_OPEN_RATE > 0) -- BAD
            ) 
        AND 
                (Click_Rate = 0 or Click_Rate > 10) -- BAD
        ) 
    THEN 'Bad'
    ELSE 'Bad' END as Recency_Frequency
from user_data
)

select * from rfm_analysis