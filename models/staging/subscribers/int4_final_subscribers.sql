with subscribers as (
    select * from {{ref('stg_subscribers')}}
),

growth_int_bucket as (
    select * from {{ref('int1_growth_int_bucket')}}
),

growth_bucket as (
    select * from {{ref('int2_growth_bucket')}}
),

churn_type as (
    select * from {{ref('int3_type_of_churn')}}
),

final_subscribers as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        STATUS,
        coalesce(Growth_Channel, 'Organic/Unknown') Growth_Channel,
        growth_int_bucket.Growth_Int_Bucket ,
        coalesce(growth_bucket.Growth_Bucket, 'Organic/Unknown') Growth_Bucket,
        churn_type.Type_of_Churn,
        coalesce(Incentivization, 'Unincentivized') Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
    LEFT JOIN growth_int_bucket using (EMAIL)
    LEFT JOIN growth_bucket using (EMAIL)
    LEFT JOIN churn_type using (EMAIL)
)

select * from final_subscribers