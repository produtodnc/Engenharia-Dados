-- back compat for old kwarg name
  
  begin;
    

        insert into AIRBNB.dev.fct_reviews ("LISTING_ID", "REVIEW_DATE", "REVIEWER_NAME", "REVIEW_TEXT", "REVIEW_SENTIMENT")
        (
            select "LISTING_ID", "REVIEW_DATE", "REVIEWER_NAME", "REVIEW_TEXT", "REVIEW_SENTIMENT"
            from AIRBNB.dev.fct_reviews__dbt_tmp
        );
    commit;