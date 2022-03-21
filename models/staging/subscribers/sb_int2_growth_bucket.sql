with subscribers as (
    select * from {{ref('sb_stg_subscribers')}}
),

organic as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        status,
        Growth_Channel,
        CASE WHEN Growth_Channel ILIKE 'Organic/Unknown' THEN 'Organic/Unknown' END Growth_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel ILIKE 'Organic/Unknown'
), 

dojo as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        status,
        Growth_Channel,
        CASE WHEN Growth_Channel ILIKE '%DojoMojo%' THEN 'Dojo' END Growth_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel ILIKE '%DojoMojo%'
), 

coreg as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        status,
        Growth_Channel,
        CASE WHEN Growth_Channel ILIKE '%CoReg%' and Growth_Channel not ilike '%mowMedia%' and Growth_Channel not ilike '%testMow%' and Growth_Channel not ilike '%NexxtHP%' and Growth_Channel not ilike '%CoReg-Campaign-Source%' THEN 'CoReg' END Growth_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel ILIKE '%CoReg%' and Growth_Channel not ilike '%mowMedia%' and Growth_Channel not ilike '%testMow%' and Growth_Channel not ilike '%NexxtHP%' and Growth_Channel not ilike '%CoReg-Campaign-Source%'
), 

influencer as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        status,
        Growth_Channel,
        CASE WHEN Growth_Channel ILIKE '%Ambassador%' THEN 'Affiliate/Influencer' END Growth_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel ILIKE '%Ambassador%'
), 

growth_from_socials as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        status,
        Growth_Channel,
        CASE WHEN Growth_Channel ILIKE '%unpaid%' and Growth_Channel ILIKE '%socialmedia%' and Growth_Channel not ilike '%newsletter%' THEN 'Growth from Socials' END Growth_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel ILIKE '%unpaid%' and Growth_Channel ILIKE '%socialmedia%' and Growth_Channel not ilike '%newsletter%'
), 

host_post as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        status,
        Growth_Channel,
        CASE WHEN Growth_Channel ilike '%mowMedia%' or Growth_Channel ilike '%testMow%' or Growth_Channel ilike '%NexxtHP%' or Growth_Channel ilike '%CoReg-Campaign-Source%' THEN 'Host & Post' END Growth_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel ilike '%mowMedia%' or Growth_Channel ilike '%testMow%' or Growth_Channel ilike '%NexxtHP%' or Growth_Channel ilike '%CoReg-Campaign-Source%'
), 

newsletters as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        status,
        Growth_Channel,
        CASE WHEN Growth_Channel ilike '%newsletter%' or Growth_Channel ilike '%paved-paid%' THEN 'Newsletters' END Growth_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel ilike '%newsletter%' or Growth_Channel ilike '%paved-paid%'
), 

other_contests as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        status,
        Growth_Channel,
        CASE WHEN Growth_Channel not ilike '%dojomojo%' and Growth_Channel not ilike '%PLN%' and Growth_Channel not ilike '%socialstance%' and Growth_Channel ilike '%contest%' THEN 'Other Contests' END Growth_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel not ilike '%dojomojo%' and Growth_Channel not ilike '%PLN%' and Growth_Channel not ilike '%socialstance%' and Growth_Channel ilike '%contest%'
), 

paid_social_media_fb as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        status,
        Growth_Channel,
        CASE WHEN Growth_Channel ilike '%socialmedia%' and Growth_Channel ilike '%paid%' and Growth_Channel not ilike '%contest%' and Growth_Channel not ilike '%unpaid%' and Growth_Channel not ilike '%TikTok%' and Growth_Channel not ilike '%snap%' and Growth_Channel not ilike '%website%' THEN 'Paid Social Media - FB' END Growth_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel ilike '%socialmedia%' and Growth_Channel ilike '%paid%' and Growth_Channel not ilike '%contest%' and Growth_Channel not ilike '%unpaid%' and Growth_Channel not ilike '%TikTok%' and Growth_Channel not ilike '%snap%' and Growth_Channel not ilike '%website%'
),

paid_social_media_tiktok as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        status,
        Growth_Channel,
        CASE WHEN Growth_Channel ilike '%socialmedia%' and Growth_Channel ilike '%paid%' and Growth_Channel ilike '%TikTok%' THEN 'Paid Social Media - TikTok' END Growth_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel ilike '%socialmedia%' and Growth_Channel ilike '%paid%' and Growth_Channel ilike '%TikTok%'
),

paid_social_media_snap as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        status,
        Growth_Channel,
        CASE WHEN Growth_Channel ilike '%socialmedia%' and Growth_Channel ilike '%paid%' and Growth_Channel ilike '%Snap%' THEN 'Paid Social Media - Snap' END Growth_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel ilike '%socialmedia%' and Growth_Channel ilike '%paid%' and Growth_Channel ilike '%Snap%'
),

referral as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        status,
        Growth_Channel,
        CASE WHEN Growth_Channel ilike '%Referral%' THEN 'Referral' END Growth_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel ilike '%Referral%'
),

scholarship as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        status,
        Growth_Channel,
        CASE WHEN Growth_Channel ilike '%Scholarship%' THEN 'Scholarship' END Growth_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel ilike '%Scholarship%'
),

search as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        status,
        Growth_Channel,
        CASE WHEN Growth_Channel ilike '%search%' THEN 'Search' END Growth_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel ilike '%search%'
),

social_stance as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        status,
        Growth_Channel,
        CASE WHEN Growth_Channel ilike '%socialstance%' THEN 'Social Stance' END Growth_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel ilike '%socialstance%'
),

student_parent_life as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        status,
        Growth_Channel,
        CASE WHEN Growth_Channel ilike '%pln%' and Growth_Channel ilike '%luckiest%' THEN 'Student Parent Life' END Growth_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel ilike '%pln%' and Growth_Channel ilike '%luckiest%'
),

website as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        status,
        Growth_Channel,
        CASE WHEN Growth_Channel ilike '%website%' and Growth_Channel not ilike '%newsletter%' THEN 'Website' END Growth_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel ilike '%website%' and Growth_Channel not ilike '%newsletter%'
),

liveintent as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        status,
        Growth_Channel,
        CASE WHEN Growth_Channel ilike '%LiveIntent%' THEN 'LiveIntent' END Growth_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel ilike '%LiveIntent%'
),

subscribers_updated as (
    select * from organic
    union
    select * from dojo
    union
    select * from coreg
    union
    select * from influencer
    union
    select * from growth_from_socials
    union
    select * from host_post
    union
    select * from newsletters
    union
    select * from other_contests
    union
    select * from paid_social_media_fb
    union
    select * from paid_social_media_tiktok
    union
    select * from paid_social_media_snap
    union
    select * from referral    
    union
    select * from scholarship
    union
    select * from search
    union
    select * from social_stance
    union
    select * from student_parent_life
    union
    select * from website 
    union
    select * from liveintent
)

select * from subscribers_updated