with sends_subscribers_unsubs as (
    select * 
    from {{ref('sb_int8_sends_subscribers_unsubs')}}
),

opens_clicks_subscribers as (
    select * 
    from {{ref('sb_int9_opens_clicks_subs_by_date')}}
),

campaign_data_by_date as (
    select 
    opens_clicks_subscribers.Campaign_Date,
    sum(sends_subscribers_unsubs.Total_Sends) Sends,
    sum(sends_subscribers_unsubs.Total_Bounced) Bounces,
    sum(sends_subscribers_unsubs.Delivered_Emails) Delivered,
    sum(opens_clicks_subscribers.Total_Opens) Total_Opens,
    sum(opens_clicks_subscribers.Gmail_Total_Opens) Gmail_Total_Opens,
    sum(opens_clicks_subscribers.Non_Gmail_Total_Opens) Non_Gmail_Total_Opens,
    sum(opens_clicks_subscribers.Unique_Opens) Unique_Opens,
    sum(opens_clicks_subscribers.Total_Opens) / sum(sends_subscribers_unsubs.Delivered_Emails) Total_Open_Rate,
    sum(opens_clicks_subscribers.Unique_Opens) / sum(sends_subscribers_unsubs.Delivered_Emails) Unique_Open_Rate,
    sum(opens_clicks_subscribers.Total_Clicks) Total_Clicks,
    sum(opens_clicks_subscribers.Unique_Clicks) Unique_Clicks,
    sum(opens_clicks_subscribers.Total_Clicks) / sum(sends_subscribers_unsubs.Delivered_Emails) Total_Click_Rate,
    sum(opens_clicks_subscribers.Unique_Clicks) / sum(sends_subscribers_unsubs.Delivered_Emails)  Unique_Click_Rate,
    sum(opens_clicks_subscribers.Total_Clicks) / sum(opens_clicks_subscribers.Total_Opens) Total_CTOR,
    sum(opens_clicks_subscribers.Unique_Clicks) / sum(opens_clicks_subscribers.Unique_Opens) Unique_CTOR,
    SUM(sends_subscribers_unsubs.total_unsubscribes) Total_Unsubscribes,
    SUM(sends_subscribers_unsubs.total_unsubscribes) / sum(sends_subscribers_unsubs.Delivered_Emails) Unsubscribe_Rate,
    SUM(sends_subscribers_unsubs.total_unsubscribes) / sum(opens_clicks_subscribers.Unique_Opens) Unsubscribe_per_Open
    from sends_subscribers_unsubs 
    LEFT JOIN opens_clicks_subscribers using (Campaign_ID, Name, Campaign_Date)
    GROUP BY 1
)

select
*,
ROUND((Unique_Open_Rate - 0.09)*Delivered*(Total_Opens / Unique_Opens)) as Adjusted_Total_Opens,
ROUND((Unique_Open_Rate - 0.09)*Delivered) as Adjusted_Unique_Opens,
Unique_Open_Rate - 0.09 as Adjusted_UOR,
(Unique_Open_Rate - 0.09)*Delivered*(Total_Opens / Unique_Opens) / Delivered as Adjusted_TOR,
Total_Clicks / ((Unique_Open_Rate - 0.09)*Delivered*(Total_Opens / Unique_Opens)) as Adjusted_CTOR,
total_unsubscribes / ((Unique_Open_Rate - 0.09)*Delivered) as Adjusted_Unsubscribe_per_Open
from campaign_data_by_date
--WHERE Campaign_Date > dateadd(month, -1, GETDATE())
ORDER BY 1