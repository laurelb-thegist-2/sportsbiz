-- boardman opens aren't filtered at this level of query. 
-- to get total opens (w/o boradman) and unique opens (w boardman) we need to filter out at a higher level.

with campaign_opens as ( 
    select 
       CAMPAIGNID AS Campaign_ID,
       City as City_of_Open,
       CountryCode as Country_Code_of_Open,
       CountryName as Country_of_Open,
       Date as timestamp,
       lower(EmailAddress) as Email,
       Region as Region_of_Open,
       LISTID as List_ID
    from analytics.CAMPAIGN_MONITOR_EVENTS.campaign_opens
    where list_id = '65ef2f913391ff42878e99dd01601196' --sports biz
)

select * from campaign_opens
