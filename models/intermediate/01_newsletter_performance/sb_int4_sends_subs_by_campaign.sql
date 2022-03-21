with sends as (
    select * from {{ref('sb_int1_sends_by_campaign')}}
),

subscribers as (
    select * from {{ref('sb_int4_final_subscribers')}}
),

sends_subscribers as (
    select
        sends.CAMPAIGN_ID,
        sends.Name,
        sends.CAMPAIGN_DATE,
        coalesce(subscribers.Country, 'US') Country,
        coalesce(subscribers.Cities, 'None') City,
        coalesce(Growth_Channel, 'Organic/Unknown') Growth_Channel, 
        coalesce(Growth_Bucket, 'Organic/Unknown') Growth_Bucket,
        coalesce(Incentivization, 'Unincentivized') Incentivization,
        count(distinct sends.email) Total_Sends,
        count(distinct sends.Bounced_Emails) Total_Bounced, --there are duplicate emails that are bounced, but not duplicates in sends. bounces per campaing_bounces doesn't match bounces due to this. 
        count(distinct sends.email) - count(distinct sends.Bounced_Emails) Delivered_Emails,
        count(distinct CASE WHEN sends.email ilike '%gmail%' THEN sends.email END) Gmail_Total_Sends,
        count(distinct CASE WHEN sends.email not ilike '%gmail%' or sends.email is NULL THEN sends.email END) Non_Gmail_Total_Sends
    from sends
    LEFT JOIN subscribers using (email)
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
    ORDER BY CAMPAIGN_DATE
)

SELECT * FROM sends_subscribers