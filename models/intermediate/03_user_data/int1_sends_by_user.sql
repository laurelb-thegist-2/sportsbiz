-- pulls from int1_sends_by_campaign in 01_newsletter_performance -> a_overall_performance

with sends as (
    select * from {{ref('int1_sends_by_campaign')}}
),

sends_by_user as (
    select
        Email,
        MIN(CAMPAIGN_DATE) FIRST_SEND,
        MAX(CAMPAIGN_DATE) MOST_RECENT_SEND,
        count(distinct sends.Campaign_ID) Total_Sends,
        count(distinct CASE WHEN sends.Bounced_Emails is NOT NULL THEN sends.CAMPAIGN_ID END) Total_Bounced--there are duplicate emails that are bounced, but not duplicates in sends. bounces per campaing_bounces doesn't match bounces due to this.  
    from sends
    GROUP BY 1
)

SELECT * FROM sends_by_user
