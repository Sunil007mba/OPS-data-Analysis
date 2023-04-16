SELECT * FROM ops_metric.`ops data`;

-- Calculate Job Reviewed per hour per day from Nov 2020
select ds,do.jobs_per_day,  do.time_in_hours from 
(select ds,count(job_id) as jobs_per_day,
        sum(time_spent)/3600 as time_in_hours
        from ops_metric.`ops data`
       where ds>="11/25/2020"
       group by 1
       order by 1 asc
       ) do
       group by 1
       
-- Calculate 7 day rolling average of throughput
SELECT *,
       avg(time_spent) 
       over(partition by job_id order by job_id 
       rows between 7 preceding and 1 following ) as rolling_average
       from ops_metric.`ops data`;
       
-- Calculate percentage share of each language in last 30 days
SELECT 
    ds,
    job_id,
    language,
    COUNT(*) * 100 / SUM(COUNT(*)) over() as percent_language
FROM
    ops_metric.`ops data`
    where ds between '11/1/2020' and '11/30/2020'
GROUP BY 1;       
       
-- Find the duplicate rows in the dataset
select *, 
      count(job_id)
      over(partition by job_id
      order by job_id) as total_count
      from  ops_metric.`ops data`
      group by job_id
      having total_count >1;
      
      
      
