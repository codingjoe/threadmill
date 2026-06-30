-- Finalize a completed task: remove it from the running set, persist the
-- result with a TTL, delete the task data hash, and add the result to the
-- per-status results history set. Evicts results whose finish score falls
-- outside the retention window (result_ttl). The per-status history sets are
-- the time series the inspector counts over for telemetry, so no separate
-- egress window or status counters are needed here.
--
-- KEYS[1]  -- running set (ZSET)
-- KEYS[2]  -- result key (STRING, stores serialized TaskResult)
-- KEYS[3]  -- task data key (HASH, deleted after acknowledge)
-- KEYS[4]  -- successful results history (ZSET, scored by finish time)
-- KEYS[5]  -- failed results history (ZSET, scored by finish time)
-- ARGV[1]  -- task ID
-- ARGV[2]  -- serialized TaskResult JSON
-- ARGV[3]  -- result TTL in seconds
-- ARGV[4]  -- finish timestamp in milliseconds (score for the history set)
-- ARGV[5]  -- status (SUCCESSFUL or FAILED)
-- Returns: 1 on success, 0 if task was not in the running set

local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed == 0 then
  return 0  -- Task already reaped, skip
end
redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3])
redis.call('DEL', KEYS[3])
local finish = tonumber(ARGV[4])
local cutoff = finish - tonumber(ARGV[3]) * 1000
local results_key = KEYS[4]
if ARGV[5] ~= 'SUCCESSFUL' then
  results_key = KEYS[5]
end
redis.call('ZADD', results_key, finish, ARGV[1])
redis.call('ZREMRANGEBYSCORE', results_key, 0, cutoff)
return 1
