-- Delete one task and its stored payload atomically.
--
-- The task hash covers pending tasks and the result key covers finished tasks,
-- so both are deleted and the missing one is a no-op.
--
-- KEYS[1]  -- status segment (ZSET, ready, running, successful, or failed)
-- KEYS[2]  -- task data key (HASH)
-- KEYS[3]  -- result key (STRING, stores serialized TaskResult)
-- ARGV[1]  -- task ID
-- Returns: 1 when the segment entry was removed, 0 otherwise

local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed == 1 then
  redis.call('DEL', KEYS[2], KEYS[3])
end
return removed
