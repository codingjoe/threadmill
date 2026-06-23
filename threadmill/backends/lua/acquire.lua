-- Atomically pop the lowest-scored task from any of the given priority queues,
-- update its JSON data with worker info, and move it directly to the running
-- set. Iterates queues in key order and returns the first available task.
--
-- KEYS[1..N]  -- interleaved running keys and queue keys, one pair per queue:
--                KEYS[1] = running set, KEYS[2] = queue set, KEYS[3] = running,
--                KEYS[4] = queue, etc.
-- ARGV[1]     -- current time in milliseconds
-- ARGV[2]     -- current time as ISO-8601 string
-- ARGV[3]     -- task key prefix (e.g. "threadmill:default:task:")
-- ARGV[4]     -- number of queue pairs (N/2)
-- ARGV[5]     -- worker name
-- ARGV[6]     -- lease TTL in milliseconds
-- Returns: updated serialized data on success, nil if all queues are empty.

local num_queues = tonumber(ARGV[4])
local lease_ttl_ms = tonumber(ARGV[6])
for i = 1, num_queues do
  local result = redis.call('ZPOPMIN', KEYS[i * 2])
  if #result > 0 then
    local task_id = result[1]
    local data = redis.call('HGET', ARGV[3] .. task_id, 'data')
    if data then
      local ok, parsed = pcall(cjson.decode, data)
      if ok then
        parsed.status = 'RUNNING'
        parsed.last_attempted_at = ARGV[2]
        if not parsed.started_at then
          parsed.started_at = ARGV[2]
        end
        if not parsed.worker_ids then
          parsed.worker_ids = {}
        end
        table.insert(parsed.worker_ids, ARGV[5])
        local updated_data = cjson.encode(parsed)
        local deadline = tonumber(ARGV[1]) + lease_ttl_ms
        redis.call('ZADD', KEYS[i * 2 - 1], deadline, task_id)
        redis.call('HSET', ARGV[3] .. task_id, 'data', updated_data)
        return updated_data
      end
    end
  end
end
return nil
