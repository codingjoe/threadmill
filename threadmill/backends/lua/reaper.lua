-- Claim expired tasks for the broker to decide. Undecided claims are picked up
-- again, and running entries without task data are dropped.
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
