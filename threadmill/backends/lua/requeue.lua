-- Atomically re-queue a failed task for a retry attempt: remove it from the
-- running and failed sets, delete its persisted result, restore its task data
-- with a fresh TTL, and schedule it in the deferred set. An optional lease
-- deadline guard makes the requeue conditional: the running set score must
-- still equal the claim the caller holds, so a stale reap decision cannot
-- resurrect a task that a worker, another broker, or the inspector already
-- handled.
--
-- KEYS[1]  -- running set (ZSET, scored by lease deadline in milliseconds)
-- KEYS[2]  -- failed results history (ZSET)
-- KEYS[3]  -- result key (STRING, deleted on requeue)
-- KEYS[4]  -- task data key (HASH)
-- KEYS[5]  -- deferred set (ZSET, scored by run_after)
-- ARGV[1]  -- task ID
-- ARGV[2]  -- serialized TaskResult JSON
-- ARGV[3]  -- priority score
-- ARGV[4]  -- run_after timestamp in milliseconds
-- ARGV[5]  -- task data TTL in seconds
-- ARGV[6]  -- telemetry pub/sub channel
-- ARGV[7]  -- queue name
-- ARGV[8]  -- expected lease deadline in milliseconds, "" to requeue unconditionally
-- Returns: 1 on success, 0 when the lease guard rejected the requeue

local lease_deadline = ARGV[8]
if lease_deadline ~= '' then
  local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
  if not score or tonumber(score) ~= tonumber(lease_deadline) then
    return 0
  end
end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('DEL', KEYS[3])
redis.call('HSET', KEYS[4], 'data', ARGV[2], 'score', ARGV[3])
redis.call('EXPIRE', KEYS[4], ARGV[5])
redis.call('ZADD', KEYS[5], ARGV[4], ARGV[1])
redis.call('PUBLISH', ARGV[6], 'ingress:' .. ARGV[7])
return 1
