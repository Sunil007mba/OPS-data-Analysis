SELECT * FROM ops_metric_data2.`table-2 events`;
select occurred_at from ops_metric_data2.`table-2 events`;

-- Calculate weekly User Engagement
SELECT CONCAT(YEAR(occurred_at), '/', WEEK(occurred_at)) AS week_name, 
       YEAR(occurred_at), WEEK(occurred_at), COUNT(user_id)
FROM     ops_metric_data2.`table-2 events`
GROUP BY week_name
ORDER BY YEAR(occurred_at) ASC, WEEK(occurred_at) ASC;
	
-- Calculate the user growth over time on product
select year(created_at), month(created_at), count(user_id),
lag(count(user_id),1) over (order by year(created_at), month(created_at)) as Prev_time_user
from ops_metric_data2.`table-1 users`
where year(created_at) in (2013,2014)
group by year(created_at), month(created_at);  

-- Calculate the weekly retention of users signup cohort
SELECT 
    a.user_id,
    a.activated,
    b.first_time,
    a.activated - b.first_time AS weekly
FROM
    (SELECT 
        user_id, week(activated_at) AS activated
    FROM
        ops_metric_data2.`table-1 users`
    GROUP BY user_id , activated) a,
    (SELECT 
        user_id, MIN(week(created_at)) AS first_time
    FROM
        ops_metric_data2.`table-1 users`
    GROUP BY user_id) b
WHERE
    a.user_id = b.user_id
    and b.first_time is not null;

-- calculate Engagement per device
select *,count(user_id)
       over(Partition by device order by user_id rows between unbounded preceding and unbounded following ) as user_per_device
       from   ops_metric_data2.`table-2 events`
       where event_type = 'engagement';
	   group by device;
       
-- calculate email engagement metric 
SELECT user_id, action, occurred_at, COUNT(user_id) over(partition by action order by user_id) as email_engagement
from ops_metric_data2.`table-3 email_events`
where action ='email_open'
group by occurred_at;
    