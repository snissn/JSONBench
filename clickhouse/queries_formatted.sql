------------------------------------------------------------------------------------------------------------------------
-- Q1 - Top event types
------------------------------------------------------------------------------------------------------------------------
SELECT
    data.commit.collection AS event,
    count() AS count
FROM bluesky
GROUP BY event
ORDER BY count DESC;

------------------------------------------------------------------------------------------------------------------------
-- Q2 - Top event types together with unique users per event type
------------------------------------------------------------------------------------------------------------------------
SELECT
    data.commit.collection AS event,
    count() AS count,
    uniqExact(data.did) AS users
FROM bluesky
WHERE data.kind = 'commit'
  AND data.commit.operation = 'create'
GROUP BY event
ORDER BY count DESC;

------------------------------------------------------------------------------------------------------------------------
-- Q3 - When do people use BlueSky
------------------------------------------------------------------------------------------------------------------------
SELECT
    data.commit.collection AS event,
    toHour(fromUnixTimestamp64Micro(data.time_us)) as hour_of_day,
    count() AS count
FROM bluesky
WHERE data.kind = 'commit'
  AND data.commit.operation = 'create'
  AND data.commit.collection in ['app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like']
GROUP BY event, hour_of_day
ORDER BY hour_of_day, event;

------------------------------------------------------------------------------------------------------------------------
-- Q4 - top 3 post veterans
------------------------------------------------------------------------------------------------------------------------
SELECT
    data.did::String as user_id,
    min(fromUnixTimestamp64Micro(data.time_us)) as first_post_ts
FROM bluesky
WHERE data.kind = 'commit'
  AND data.commit.operation = 'create'
  AND data.commit.collection = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY first_post_ts ASC
LIMIT 3;

------------------------------------------------------------------------------------------------------------------------
-- Q5 - top 3 users with longest activity
------------------------------------------------------------------------------------------------------------------------
SELECT
    data.did::String as user_id,
    date_diff(
        'milliseconds',
        min(fromUnixTimestamp64Micro(data.time_us)),
        max(fromUnixTimestamp64Micro(data.time_us))) AS activity_span
FROM bluesky
WHERE data.kind = 'commit'
  AND data.commit.operation = 'create'
  AND data.commit.collection = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY activity_span DESC
LIMIT 3;

------------------------------------------------------------------------------------------------------------------------
-- QEXPR - arbitrary expression scan over typed numeric time
------------------------------------------------------------------------------------------------------------------------
WITH
    intDiv(data.time_us, 1000000)
        - if(data.time_us < 0 AND data.time_us != intDiv(data.time_us, 1000000) * 1000000, 1, 0) AS unix_seconds,
    unix_seconds - intDiv(unix_seconds, 86400) * 86400 AS second_of_day_raw,
    if(second_of_day_raw < 0, second_of_day_raw + 86400, second_of_day_raw) AS second_of_day
SELECT
    sum(toInt64(second_of_day) * toInt64(second_of_day)) AS second_of_day_square_sum
FROM bluesky;
