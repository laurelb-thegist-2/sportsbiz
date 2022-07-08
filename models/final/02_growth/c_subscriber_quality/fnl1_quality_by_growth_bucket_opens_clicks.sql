with opens_quality as (
    select * from {{ref('sb_int1_recency_frequency')}}
),

quality_by_growth_bucket as (
    select
        Growth_Bucket,
        SUM(CASE WHEN Recency_Frequency = 'Good' THEN 1 ELSE 0 END) as Good_Subscribers,
        SUM(CASE WHEN Recency_Frequency = 'Medium' THEN 1 ELSE 0 END) as Medium_Subscribers,
        SUM(CASE WHEN Recency_Frequency = 'Bad' THEN 1 ELSE 0 END) as Bad_Subscribers
    from opens_quality
    where status = 'Active'
    group by 1
)

select * from quality_by_growth_bucket
order by 1