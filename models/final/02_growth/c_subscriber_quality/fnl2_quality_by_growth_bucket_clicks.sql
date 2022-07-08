with clicks_quality as (
    select * from {{ref('sb_int3_clicks_recency_frequency')}}
),

clicks_quality_by_growth_bucket as (
    select
        Growth_Bucket,
        SUM(CASE WHEN Recency_Frequency = 'Good' THEN 1 ELSE 0 END) as Good_Subscribers,
        SUM(CASE WHEN Recency_Frequency = 'Medium' THEN 1 ELSE 0 END) as Medium_Subscribers,
        SUM(CASE WHEN Recency_Frequency = 'Bad' THEN 1 ELSE 0 END) as Bad_Subscribers
    from clicks_quality
    where status = 'Active'
    group by 1
)

select * from clicks_quality_by_growth_bucket
order by 1