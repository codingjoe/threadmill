-- Fail tasks whose processing lease has expired.
--
-- KEYS[1]  -- running set (ZSET)
-- KEYS[2]  -- results history set (ZSET)
-- KEYS[3]  -- egress window (ZSET, may be empty)
-- KEYS[4]  -- failed counter (STRING, may be empty)
-- ARGV[1]  -- current time in milliseconds (for score comparison)
-- ARGV[2]  -- task key prefix (e.g. "threadmill:default:task:")
-- ARGV[3]  -- result key prefix (e.g. "threadmill:default:result:")
-- ARGV[4]  -- batch size
-- ARGV[5]  -- result TTL in seconds
-- ARGV[6]  -- finished_at as ISO format string
-- Returns: number of tasks failed

local stale = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'LIMIT', 0, tonumber(ARGV[4]))
for _, task_id in ipairs(stale) do
  local data = redis.call('HGET', ARGV[2] .. task_id, 'data')
  if data then
    local ok, parsed = pcall(cjson.decode, data)
    if ok then
      parsed.status = 'FAILED'
      parsed.finished_at = ARGV[6]
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
      redis.call('DEL', ARGV[2] .. task_id)
      redis.call('ZADD', KEYS[2], tonumber(ARGV[1]), task_id)
      redis.call('ZADD', KEYS[3], ARGV[1], task_id)
      redis.call('INCR', KEYS[4])
    end
  end
end
return #stale
