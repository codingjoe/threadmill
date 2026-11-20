-- Delete every payload of one queue together with its segment keys, atomically.
--
-- Pending members address task hashes and finished members address result keys,
-- so the payload of every segment entry is removed before the sets disappear.
--
-- KEYS[1]  -- ready set (ZSET of task IDs)
-- KEYS[2]  -- running set (ZSET of task IDs)
-- KEYS[3]  -- deferred set (ZSET of task IDs)
-- KEYS[4]  -- successful results history (ZSET of result IDs)
-- KEYS[5]  -- failed results history (ZSET of result IDs)
-- ARGV[1]  -- task key prefix (e.g. "threadmill:{default}:task:")
-- ARGV[2]  -- result key prefix (e.g. "threadmill:{default}:result:")
-- Returns: number of deleted payloads

local function delete_payloads(set_key, prefix)
  local deleted = 0
  for _, id in ipairs(redis.call('ZRANGE', set_key, 0, -1)) do
    deleted = deleted + redis.call('DEL', prefix .. id)
  end
  return deleted
end

local deleted = 0
for index = 1, 3 do
  deleted = deleted + delete_payloads(KEYS[index], ARGV[1])
end
for index = 4, 5 do
  deleted = deleted + delete_payloads(KEYS[index], ARGV[2])
end
redis.call('DEL', KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5])
return deleted
