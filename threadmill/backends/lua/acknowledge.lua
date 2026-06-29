-- Finalize a completed task: remove it from the running set, persist the
-- result with a TTL, delete the task data hash, and add the result to the
-- results history set. Also evict results whose finish score falls outside the
-- retention window. Records egress and status counters so the inspector TUI can
-- show per-queue telemetry without scanning the full result history.
--
-- KEYS[1]  -- running set (ZSET)
-- KEYS[2]  -- result key (STRING, stores serialized TaskResult)
-- KEYS[3]  -- task data key (HASH, deleted after acknowledge)
-- KEYS[4]  -- results history set (ZSET, ordered by finish time)
-- KEYS[5]  -- egress window (ZSET, members are task IDs scored by finish time)
-- KEYS[6]  -- successful counter (STRING, may be empty)
-- KEYS[7]  -- failed counter (STRING, may be empty)
-- ARGV[1]  -- task ID
-- ARGV[2]  -- serialized TaskResult JSON
-- ARGV[3]  -- result TTL in seconds
-- ARGV[4]  -- finish timestamp in milliseconds (score for the results set)
-- ARGV[5]  -- status (SUCCESSFUL or FAILED)
-- Returns: 1 on success, 0 if task was not in the running set

local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed == 0 then
  return 0  -- Task already reaped, skip
end
redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3])
redis.call('DEL', KEYS[3])
redis.call('ZADD', KEYS[4], ARGV[4], ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[4], 0, tonumber(ARGV[4]) - tonumber(ARGV[3]) * 1000)
redis.call('ZADD', KEYS[5], ARGV[4], ARGV[1])
if ARGV[5] == 'SUCCESSFUL' then
  redis.call('INCR', KEYS[6])
elseif ARGV[5] == 'FAILED' then
  redis.call('INCR', KEYS[7])
end
return 1
