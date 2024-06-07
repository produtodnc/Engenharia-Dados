
with  __dbt__cte__src_reviews as (
with raw_reviews as (select * from AIRBNB.raw.raw_reviews)
select
    listing_id,
    date as review_date,
    reviewer_name,
    comments as review_text,
    sentiment as review_sentiment
from raw_reviews
), src_reviews as (select * from __dbt__cte__src_reviews)
select *
from src_reviews
where
    review_text is not null
    
        and review_date > (select max(review_date) from AIRBNB.dev.fct_reviews)
    