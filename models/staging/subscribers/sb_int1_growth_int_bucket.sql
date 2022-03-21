with subscribers as (
    select * from {{ref('sb_stg_subscribers')}}
),

DVM as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        STATUS,
        Growth_Channel,
        CASE WHEN Growth_Channel ILIKE '%digitalviking-DVM%' THEN 'DVM' END Growth_Int_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel ILIKE '%digitalviking-DVM%'
), 

advisio_kubient as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        STATUS,
        Growth_Channel,
        CASE WHEN Growth_Channel ILIKE '%advisio%' or Growth_Channel ilike '%KUBTG03%' THEN 'Advisio/Kubient' END Growth_Int_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel ILIKE '%advisio%' or Growth_Channel ilike '%KUBTG03%'
), 

DMI as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        STATUS,
        Growth_Channel,
        CASE WHEN Growth_Channel ILIKE '%dmipartners%' THEN 'DMI' END Growth_Int_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel ILIKE '%dmipartners%' 
), 

DMS as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        STATUS,
        Growth_Channel,
        CASE WHEN Growth_Channel ILIKE '%CoReg-DMS%' THEN 'DMS' END Growth_Int_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel ILIKE '%CoReg-DMS%'
), 

leadpulse as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        STATUS,
        Growth_Channel,
        CASE WHEN Growth_Channel ILIKE '%leadpulse%' THEN 'Leadpulse' END Growth_Int_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel ILIKE '%leadpulse%'
), 

NexxtHP as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        STATUS,
        Growth_Channel,
        CASE WHEN Growth_Channel ILIKE '%NexxtHP%' THEN 'NexxtHP' END Growth_Int_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel ILIKE '%NexxtHP%'
), 

LENHP as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        STATUS,
        Growth_Channel,
        CASE WHEN Growth_Channel ILIKE '%LENHP%' or Growth_Channel ilike '%lensa%' THEN 'LENHP' END Growth_Int_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    WHERE Growth_Channel ILIKE '%LENHP%' or Growth_Channel ilike '%lensa%'
),

growth_channels_shortened as (
    select * from DVM
    union
    select * from DMI
    union
    select * from DMS
    union
    select * from advisio_kubient
    union 
    select * from leadpulse
    union
    select * from LENHP
    union 
    select * from NexxtHP
)

select * from growth_channels_shortened