with campaign_clicks as ( 
    select 
       CAMPAIGNID AS Campaign_ID,
       City as City_of_Click,
       CountryCode as Country_Code_of_Click,
       CountryName as Country_of_Click,
       Date as timestamp,
       lower(EmailAddress) as Email,
       Region as Region_of_Click,
       LISTID as List_ID,
       URL
    from analytics.CAMPAIGN_MONITOR_EVENTS.campaign_clicks
    where URL is not NULL and (city_of_click != 'Boardman' or city_of_click is NULL) 
    and list_id = '65ef2f913391ff42878e99dd01601196' --sports biz
)

select * from campaign_clicks
WHERE 
URL ilike '%sponsorpulse%' or
URL ilike '%campaignmonitor%'

