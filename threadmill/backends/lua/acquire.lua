-- Lease the next ready task for the calling worker: returns the task ID and its
-- stored payload, or nil when no queue has a task. Queues are tried round-robin
-- from ARGV[7], so a backlogged queue cannot starve its neighbours.
--
-- The payload comes back as it was enqueued. Apply the lease stamped beside it
-- (lease_worker, lease_started_at) to report the task as RUNNING.
--
-- KEYS[1..N]  -- interleaved running keys and queue keys, one pair per queue:
--                KEYS[1] = running set, KEYS[2] = queue set, KEYS[3] = running,
--                KEYS[4] = queue, etc.
-- ARGV[1]     -- current time in milliseconds
-- ARGV[2]     -- current time as ISO-8601 string, stamped as lease_started_at
-- ARGV[3]     -- task key prefix (e.g. "threadmill:task:")
-- ARGV[4]     -- number of queue pairs (N/2)
-- ARGV[5]     -- worker name, stamped as lease_worker
-- ARGV[6]     -- lease TTL in milliseconds
-- ARGV[7]     -- start_index; 0-based index of the queue pair to scan first, so
--                start_index 0 is the pair at KEYS[1] and KEYS[2]
-- Returns: the task ID and its stored data, or nil when no queue yields a task.
-- An entry whose hash holds no data returns nil too, leaving its queue unleased.

local num_queues = tonumber(ARGV[4])
local lease_ttl_ms = tonumber(ARGV[6])
local start_index = tonumber(ARGV[7])
for offset = 0, num_queues - 1 do
  local queue_index = (start_index + offset) % num_queues + 1
  local result = redis.call('ZPOPMIN', KEYS[queue_index * 2])
  if #result > 0 then
    local task_id = result[1]
    local task_key = ARGV[3] .. task_id
    local data = redis.call('HGET', task_key, 'data')
    if data then
      local deadline = tonumber(ARGV[1]) + lease_ttl_ms
      redis.call('ZADD', KEYS[queue_index * 2 - 1], deadline, task_id)
      redis.call('HSET', task_key, 'lease_worker', ARGV[5], 'lease_started_at', ARGV[2])
      return {task_id, data}
    end
  end
end
return nil
