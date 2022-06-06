with SUBSCRIBERS as (
    select * from {{ref('sb_int4_final_subscribers')}}
),

lost_subscribers as (
SELECT 
    date_status_changed Date,
    Growth_Channel,
    Growth_Int_Bucket,
    Growth_Bucket,
    Incentivization,
    Type_of_Churn,
    status,
    coalesce(Country, 'US') Country,
    coalesce(Cities, 'None') Cities,
    source_brand,
    campaign_name,
    -1*(count(EMAIL)) as Churn
FROM SUBSCRIBERS 
WHERE date_status_changed > dateadd(day, -8, GETDATE()) --Sunday
AND date_status_changed < GETDATE() --Monday
AND status <> 'Active'
Group by 1,2,3,4,5,6,7,8,9,10,11
ORDER BY 1 DESC
)

select * from lost_subscribers
limit 100000