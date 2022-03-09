with subscribers as ( 
    select 
        lower(EMAILADDRESS) AS EMAIL,
        LEADID,
        LISTID as list_ID,
        cast("DATE" as DATE) date_status_changed,
        STATUS,
        coalesce(GROWTHCHANNEL, 'Organic/Unknown') as Growth_Channel,
        CASE WHEN Growth_Channel ilike '%dojo%' or Growth_Channel ilike '%contest%' or Growth_Channel ilike '%coreg%' or Growth_Channel ilike '%co-reg%' or Growth_Channel ilike '%Scholarship%' or Growth_Channel ilike '%socialstance%' or Growth_Channel ilike '%PLN%' THEN 'Incentivized' ELSE 'Unincentivized' END Incentivization,
        COUNTRY as country,
        CITIES,
        REFERRALCODE as referral_code,
        REFERRALCOUNT as referral_count,
        CAMPAIGNNAME as campaign_name,
        SOURCEBRAND as source_brand,
        PARTNERENGAGEMENTSURVEYS as Partner_Engagement_Surveys
    from analytics.core.all_subscribers 
    where list_id = '65ef2f913391ff42878e99dd01601196' --sports biz
)

select * from subscribers