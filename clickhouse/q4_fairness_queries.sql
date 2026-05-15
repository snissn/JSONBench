-- q4a aggregate over the time-ordered table created by RUN_Q4_FAIRNESS=1.
SELECT data.did::String as user_id, min(fromUnixTimestamp64Micro(data.time_us)) as first_post_ts FROM bluesky_q4_time WHERE data.kind = 'commit' AND data.commit.operation = 'create' AND data.commit.collection = 'app.bsky.feed.post' GROUP BY user_id ORDER BY first_post_ts ASC LIMIT 3;

-- q4a streaming-shaped query over the same time-ordered table.
SELECT user_id, first_post_ts FROM (SELECT data.did::String AS user_id, fromUnixTimestamp64Micro(data.time_us) AS first_post_ts FROM bluesky_q4_time WHERE data.kind = 'commit' AND data.commit.operation = 'create' AND data.commit.collection = 'app.bsky.feed.post' ORDER BY first_post_ts ASC, user_id ASC LIMIT 1 BY user_id) ORDER BY first_post_ts ASC, user_id ASC LIMIT 3;

-- q4b aggregate over the standard ClickHouse table order.
SELECT data.did::String as user_id, min(fromUnixTimestamp64Micro(data.time_us)) as first_post_ts FROM bluesky WHERE data.kind = 'commit' AND data.commit.operation = 'create' AND data.commit.collection = 'app.bsky.feed.post' GROUP BY user_id ORDER BY first_post_ts ASC LIMIT 3;
