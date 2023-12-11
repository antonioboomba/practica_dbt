WITH date_dimension as (
  {{ dbt_date.get_date_dimension(
      start_date='2018-12-31',
      end_date='2025-12-31'
  ) }}
)


SELECT 
    date_day,
    prior_date_day,
    next_date_day,
    day_of_week,
FROM date_dimension