-- Claim tasks whose processing lease has expired so the broker can decide
-- whether the retry callback requeues them or they are finalized as failed.
-- Claiming renews the lease to the claim deadline: the task stays in the
-- running set, so a concurrent broker pass cannot take it over, and keeps its
-- task data hash, so the broker can deserialize it and evaluate the retry
-- callback. Should the broker stop before deciding, the claim lapses and the
-- next pass claims the task again. Running entries without task data are
-- unrecoverable and removed.
--
-- KEYS[1]  -- running set (ZSET, scored by lease deadline in milliseconds)
-- ARGV[1]  -- current time in milliseconds (all scores <= this are expired)
-- ARGV[2]  -- claim deadline in milliseconds
-- ARGV[3]  -- maximum number of tasks to claim per call (batch size)
-- ARGV[4]  -- task key prefix (e.g. "threadmill:default:task:")
-- Returns: list of claimed task IDs

local expired = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'LIMIT', 0, tonumber(ARGV[3]))
local claimed = {}
for _, task_id in ipairs(expired) do
  if redis.call('EXISTS', ARGV[4] .. task_id) == 1 then
    redis.call('ZADD', KEYS[1], ARGV[2], task_id)
    table.insert(claimed, task_id)
  else
    redis.call('ZREM', KEYS[1], task_id)
  end
end
return claimed
