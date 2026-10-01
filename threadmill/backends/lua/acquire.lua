-- Atomically pop up to ARGV[8] lowest-scored tasks from any of the given
-- priority queues, update their JSON data with worker info, and move them
-- directly to the running set. Scans the queues round-robin from ARGV[7], one
-- queue per pop, so a batch spreads across queues instead of draining one and a
-- backlogged queue cannot starve its neighbours.
--
-- KEYS[1..N]  -- interleaved running keys and queue keys, one pair per queue:
--                KEYS[1] = running set, KEYS[2] = queue set, KEYS[3] = running,
--                KEYS[4] = queue, etc.
-- ARGV[1]     -- current time in milliseconds
-- ARGV[2]     -- current time as ISO-8601 string
-- ARGV[3]     -- task key prefix (e.g. "threadmill:task:")
-- ARGV[4]     -- number of queue pairs (N/2)
-- ARGV[5]     -- worker name
-- ARGV[6]     -- lease TTL in milliseconds
-- ARGV[7]     -- start_index; 0-based index of the queue pair to scan first, so
--                start_index 0 is the pair at KEYS[1] and KEYS[2]
-- ARGV[8]     -- maximum number of tasks to pop
-- Returns: array of updated serialized task data, empty if all queues are empty.

local num_queues = tonumber(ARGV[4])
local lease_ttl_ms = tonumber(ARGV[6])
local start_index = tonumber(ARGV[7])
local max_count = tonumber(ARGV[8])
local task_key_prefix = ARGV[3]
local now_ms = tonumber(ARGV[1])
local tasks = {}
local misses = 0
local queue_index = start_index % num_queues + 1
-- joe: one full round without a task ends the scan, so a queue whose head
-- entry is a ghost yields a short batch; retry the queue if ghosts ever dominate
while #tasks < max_count and misses < num_queues do
  local result = redis.call('ZPOPMIN', KEYS[queue_index * 2])
  misses = misses + 1
  local data = #result > 0
    and redis.call('HGET', task_key_prefix .. result[1], 'data')
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
      local deadline = now_ms + lease_ttl_ms
      redis.call('ZADD', KEYS[queue_index * 2 - 1], deadline, result[1])
      redis.call('HSET', task_key_prefix .. result[1], 'data', updated_data)
      table.insert(tasks, updated_data)
      misses = 0
    end
  end
  queue_index = queue_index % num_queues + 1
end
return tasks
