with campaign_clicks as (
    select * from {{ref('sb_stg_campaign_clicks')}}
),

campaign_details as (
    select * from {{ref('sb_stg_campaign_details')}}
),

campaign_clicks_details as (
    select
        campaign_details.Campaign_Date,
        Campaign_ID,
        Email,
        City_of_Click,
        URL,
        Timestamp
    from campaign_clicks  
    JOIN campaign_details using (Campaign_ID)
)

SELECT * FROM campaign_clicks_details