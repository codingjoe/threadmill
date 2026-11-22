#!lua name=threadmill

local TASK_DATA_FIELD = 'data'
local TASK_SCORE_FIELD = 'score'
local LEASE_WORKER_FIELD = 'lease_worker'
local LEASE_STARTED_AT_FIELD = 'lease_started_at'
local LEASE_TOKEN_FIELD = 'lease_token'
local STATUS_SUCCESSFUL = 'SUCCESSFUL'

local function task_key(prefix, task_id)
  return prefix .. task_id
end

local function acquire_tasks(queue_keys, now_ms, lease_started_at_iso,
    task_key_prefix, queue_count, worker, lease_ttl_ms, start_index, max_count)
  local tasks = {}
  local misses = 0
  local queue_index = start_index % queue_count + 1
  -- joe: one full round without a task ends the scan, so a queue whose head
  -- entry is a ghost yields a short batch; retry the queue if ghosts ever dominate
  while #tasks < max_count and misses < queue_count do
    local popped = redis.call('ZPOPMIN', queue_keys[queue_index * 2])
    local data
    if #popped > 0 then
      local key = task_key(task_key_prefix, popped[1])
      data = redis.call('HGET', key, TASK_DATA_FIELD)
      if data then
        local lease_token = string.format(
          '%06x%06x%06x%06x',
          math.random(0, 0xffffff), math.random(0, 0xffffff),
          math.random(0, 0xffffff), math.random(0, 0xffffff))
        redis.call('ZADD', queue_keys[queue_index * 2 - 1],
          now_ms + lease_ttl_ms, popped[1])
        redis.call('HSET', key,
          LEASE_WORKER_FIELD, worker,
          LEASE_STARTED_AT_FIELD, lease_started_at_iso,
          LEASE_TOKEN_FIELD, lease_token)
        table.insert(tasks, { popped[1], data, lease_token, queue_index - 1 })
      end
    end
    misses = data and 0 or (misses + 1)
    queue_index = queue_index % queue_count + 1
  end
  return tasks
end

local function acknowledge_task(running_key, result_key, task_data_key,
    successful_results_key, failed_results_key, task_id, serialized_result,
    result_ttl_secs, finished_at_ms, status, telemetry_channel, queue_name,
    lease_token)
  local stored_lease_token = redis.call(
    'HGET', task_data_key, LEASE_TOKEN_FIELD)
  if stored_lease_token and stored_lease_token ~= lease_token then
    return 0
  end

  if redis.call('ZREM', running_key, task_id) == 0 then
    return 0  -- Task already reaped, skip
  end
  redis.call('SET', result_key, serialized_result, 'EX', result_ttl_secs)
  redis.call('DEL', task_data_key)
  local cutoff_ms = finished_at_ms - result_ttl_secs * 1000
  local results_key = successful_results_key
  if status ~= STATUS_SUCCESSFUL then
    results_key = failed_results_key
  end
  redis.call('ZADD', results_key, finished_at_ms, task_id)
  redis.call('ZREMRANGEBYSCORE', results_key, 0, cutoff_ms)
  redis.call('PUBLISH', telemetry_channel, 'egress:' .. queue_name)
  return 1
end

local function delete_task(segment_key, task_data_key, result_key, task_id)
  local removed = redis.call('ZREM', segment_key, task_id)
  if removed == 1 then
    redis.call('DEL', task_data_key, result_key)
  end
  return removed
end

local function move_due_tasks(deferred_key, ready_key, now_ms,
    task_key_prefix, max_count)
  local due_ids = redis.call('ZRANGEBYSCORE', deferred_key,
    0, now_ms, 'LIMIT', 0, max_count)
  local dropped_ids = {}
  for _, task_id in ipairs(due_ids) do
    redis.call('ZREM', deferred_key, task_id)
    local score = redis.call('HGET',
      task_key(task_key_prefix, task_id), TASK_SCORE_FIELD)
    if score then
      redis.call('ZADD', ready_key, score, task_id)
    else
      table.insert(dropped_ids, task_id)
    end
  end
  return dropped_ids
end

local function purge_queue(ready_key, running_key, deferred_key,
    successful_results_key, failed_results_key, task_key_prefix,
    result_key_prefix)
  local function delete_payloads(set_key, key_prefix)
    local deleted = 0
    for _, id in ipairs(redis.call('ZRANGE', set_key, 0, -1)) do
      deleted = deleted + redis.call('DEL', task_key(key_prefix, id))
    end
    return deleted
  end

  local deleted = 0
  for _, set_key in ipairs({ready_key, running_key, deferred_key}) do
    deleted = deleted + delete_payloads(set_key, task_key_prefix)
  end
  for _, set_key in ipairs({successful_results_key, failed_results_key}) do
    deleted = deleted + delete_payloads(set_key, result_key_prefix)
  end
  redis.call('DEL', ready_key, running_key, deferred_key,
    successful_results_key, failed_results_key)
  return deleted
end

local function reap_expired_tasks(running_key, claim_ttl_ms, max_count,
    task_key_prefix)
  local clock = redis.call('TIME')
  local now_ms = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
  local claim_deadline_ms = now_ms + claim_ttl_ms
  local expired_ids = redis.call('ZRANGEBYSCORE', running_key,
    0, now_ms, 'LIMIT', 0, max_count)
  local claimed_ids = {}
  for _, task_id in ipairs(expired_ids) do
    if redis.call('EXISTS', task_key(task_key_prefix, task_id)) == 1 then
      redis.call('ZADD', running_key, claim_deadline_ms, task_id)
      table.insert(claimed_ids, task_id)
    else
      redis.call('ZREM', running_key, task_id)
    end
  end
  return claimed_ids
end

redis.register_function('threadmill_acquire', function(keys, args)
  return acquire_tasks(keys, tonumber(args[1]), args[2], args[3],
    tonumber(args[4]), args[5], tonumber(args[6]), tonumber(args[7]),
    tonumber(args[8]))
end)

redis.register_function('threadmill_acknowledge', function(keys, args)
  return acknowledge_task(keys[1], keys[2], keys[3], keys[4], keys[5],
    args[1], args[2], tonumber(args[3]), tonumber(args[4]), args[5], args[6],
    args[7], args[8])
end)

redis.register_function('threadmill_dequeue', function(keys, args)
  return delete_task(keys[1], keys[2], keys[3], args[1])
end)

redis.register_function('threadmill_mover', function(keys, args)
  return move_due_tasks(keys[1], keys[2], tonumber(args[1]), args[2],
    tonumber(args[3]))
end)

redis.register_function('threadmill_purge', function(keys, args)
  return purge_queue(keys[1], keys[2], keys[3], keys[4], keys[5], args[1],
    args[2])
end)

redis.register_function('threadmill_reaper', function(keys, args)
  return reap_expired_tasks(keys[1], tonumber(args[1]), tonumber(args[2]),
    args[3])
end)
