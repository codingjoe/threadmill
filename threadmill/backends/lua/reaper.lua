-- Fail tasks whose processing lease has expired. The lease the acquire script
-- stamped onto the task hash is folded into the failure, so a reaped result
-- shows who held the lease and when the attempt started. Reaped tasks are
-- recorded in the failed results history (the time series the inspector counts
-- over), then evicted once older than result_ttl.
--
-- KEYS[1]  -- running set (ZSET)
-- KEYS[2]  -- failed results history (ZSET, scored by finish time)
-- ARGV[1]  -- current time in milliseconds (for score comparison)
-- ARGV[2]  -- task key prefix (e.g. "threadmill:default:task:")
-- ARGV[3]  -- result key prefix (e.g. "threadmill:default:result:")
-- ARGV[4]  -- batch size
-- ARGV[5]  -- result TTL in seconds
-- ARGV[6]  -- finished_at as ISO format string
-- Returns: number of tasks failed

local stale = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'LIMIT', 0, tonumber(ARGV[4]))
for _, task_id in ipairs(stale) do
  local task_key = ARGV[2] .. task_id
  local stored = redis.call('HMGET', task_key, 'data', 'lease_worker', 'lease_started_at')
  if stored[1] then
    local ok, parsed = pcall(cjson.decode, stored[1])
    if ok then
      parsed.status = 'FAILED'
      parsed.finished_at = ARGV[6]
      if stored[3] then
        parsed.last_attempted_at = stored[3]
        if parsed.started_at == nil or parsed.started_at == cjson.null then
          parsed.started_at = stored[3]
        end
      end
      if stored[2] and stored[2] ~= '' then
        if not parsed.worker_ids then
          parsed.worker_ids = {}
        end
        table.insert(parsed.worker_ids, stored[2])
      end
      if not parsed.errors then
        parsed.errors = {}
      end
      table.insert(parsed.errors, {
        exception_class_path = 'threadmill.exceptions.AcknowledgementTimeout',
        traceback = 'Task processing lease expired.'
      })
      local failed_data = cjson.encode(parsed)
      redis.call('ZREM', KEYS[1], task_id)
      redis.call('SET', ARGV[3] .. task_id, failed_data, 'EX', ARGV[5])
      redis.call('DEL', task_key)
      redis.call('ZADD', KEYS[2], tonumber(ARGV[1]), task_id)
    end
  end
end
-- Evict failed results older than result_ttl to bound the history.
redis.call('ZREMRANGEBYSCORE', KEYS[2], 0, tonumber(ARGV[1]) - tonumber(ARGV[5]) * 1000)
return #stale
