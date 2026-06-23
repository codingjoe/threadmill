-- Move tasks whose scheduled time has passed from the deferred set to the active
-- priority queue. Only processes up to a batch limit per call.
--
-- KEYS[1]  -- deferred set (ZSET, scored by run_after timestamp)
-- KEYS[2]  -- active priority queue (ZSET, scored by priority+time)
-- ARGV[1]  -- current time in milliseconds (all scores <= this are due)
-- ARGV[2]  -- task key prefix (e.g. "threadmill:default:task:")
-- ARGV[3]  -- maximum number of tasks to move per call (batch size)
-- Returns: number of tasks moved

local due = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'LIMIT', 0, tonumber(ARGV[3]))
for _, task_id in ipairs(due) do
  redis.call('ZREM', KEYS[1], task_id)
  local score = redis.call('HGET', ARGV[2] .. task_id, 'score')
  if score then
    redis.call('ZADD', KEYS[2], score, task_id)
  end
end
return #due
