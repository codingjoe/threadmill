-- Lease up to ARGV[8] ready tasks for the calling worker in one round-trip:
-- each task's stored payload comes back beside the lease token stamped for it.
-- Queues are tried round-robin from ARGV[7], one queue per pop, so a batch
-- spreads across queues instead of draining one and a backlogged queue cannot
-- starve its neighbours.
--
-- The payload comes back as it was enqueued. Apply the lease stamped beside it
-- (lease_worker, lease_started_at, lease_token) to report the task as RUNNING.
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
-- ARGV[8]     -- maximum number of tasks to lease
-- Returns: array of {payload, lease_token} pairs, empty when no queue yields a
-- task. A queue whose head entry holds no payload yields nothing for that pop
-- and the scan moves on.

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
  local popped = redis.call('ZPOPMIN', KEYS[queue_index * 2])
  local data = #popped > 0
    and redis.call('HGET', task_key_prefix .. popped[1], 'data')
  if data then
    local lease_token = string.format(
      '%06x%06x%06x%06x',
      math.random(0, 0xffffff), math.random(0, 0xffffff),
      math.random(0, 0xffffff), math.random(0, 0xffffff))
    local deadline = now_ms + lease_ttl_ms
    redis.call('ZADD', KEYS[queue_index * 2 - 1], deadline, popped[1])
    redis.call('HSET', task_key_prefix .. popped[1],
      'lease_worker', ARGV[5],
      'lease_started_at', ARGV[2],
      'lease_token', lease_token)
    table.insert(tasks, { data, lease_token })
  end
  misses = data and 0 or (misses + 1)
  queue_index = queue_index % num_queues + 1
end
return tasks
