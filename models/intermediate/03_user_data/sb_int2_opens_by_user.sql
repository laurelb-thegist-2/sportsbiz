-- pulls from int2_opens_by_campaign (intermediate -> 01_newsletter_performance -> a_overall_performance), 
-- which combines campaign_opens with campaign_details
-- boardman opens are filtered out from this query for total opens, but not unique opens

with opens as (
    select * from {{ref('sb_int2_opens_by_campaign')}}
),

opens_by_user as (
    select 
        Email,
        Campaign_ID,
        Campaign_Date,
        Name,
        MIN(Timestamp) First_Open,
        MAX(Timestamp) Most_Recent_Open,
        count(CASE WHEN City_of_Open != 'Boardman' or city_of_open is NULL THEN email END) Total_Opens,
        count(distinct email) Unique_Opens
    from opens
    GROUP BY 1, 2, 3, 4
)

select * from opens_by_user