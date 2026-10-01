-- Claim tasks whose processing lease has expired so the broker can decide
-- whether the retry callback requeues them or they are finalized as failed.
-- Claiming renews the lease to now + claim TTL: the task stays in the running
-- set, so a concurrent broker pass cannot take it over, and keeps its task data
-- hash, so the broker can deserialize it and evaluate the retry callback.
-- Should the broker stop before deciding, the claim lapses and the next pass
-- claims the task again. Running entries without task data are unrecoverable
-- and removed. "Now" comes from the Redis server clock, so the broker's own
-- clock does not affect lease expiry.
--
-- KEYS[1]  -- running set (ZSET, scored by lease deadline in milliseconds)
-- ARGV[1]  -- claim TTL in milliseconds
-- ARGV[2]  -- maximum number of tasks to claim per call (batch size)
-- ARGV[3]  -- task key prefix (e.g. "threadmill:default:task:")
-- Returns: list of claimed task IDs

local clock = redis.call('TIME')
local now_ms = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local claim_deadline_ms = now_ms + tonumber(ARGV[1])
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], 0, now_ms, 'LIMIT', 0, tonumber(ARGV[2]))
local claimed = {}
for _, task_id in ipairs(expired) do
  if redis.call('EXISTS', ARGV[3] .. task_id) == 1 then
    redis.call('ZADD', KEYS[1], claim_deadline_ms, task_id)
    table.insert(claimed, task_id)
  else
    redis.call('ZREM', KEYS[1], task_id)
  end
end
return claimed
