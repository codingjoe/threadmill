# Contributing

When writing code, you MUST ALWAYS follow the [naming-things](https://github.com/codingjoe/naming-things/blob/main/README.md) guidelines.

<!--
Bots/Agents MUST use:
curl -sSL https://raw.githubusercontent.com/codingjoe/naming-things/refs/heads/main/README.md
-->

## Design Principles

- Durability – We recover from any failures, even poorly written tasks.
- Consistency – We never lose data, even if someone unplugs the power or network.
- Utilization – We keep the CPU saturated with tasks, not with idle time or waiting for locks.

We require a persistent Redis without eviction.

Redis connections explicitly load with `decode_responses=True`, so all values
read from Redis are strings. We do not guard against Redis data altered
mid-flight. We fail loudly instead. These are deliberate design decisions.

## Task lifecycle

A task lives in one of five Redis sets. READY and DEFERRED hold pending
work. RUNNING holds leased tasks. SUCCESSFUL and FAILED hold published
results.

```mermaid
stateDiagram-v2
    direction LR
    state "Successful (terminal)" as Successful

    [*] --> Ready: enqueue
    [*] --> Deferred: enqueue with run_after
    Deferred --> Ready: run_after reached (broker)
    Ready --> Running: acquired with a lease

    Running --> Deferred: raised, retry delay returned
    Running --> Deferred: lease expired, retry delay returned
    Deferred --> Ready: retry delay elapsed (broker)

    Running --> Successful: returned
    Running --> Failed: raised, retry returns None or raises
    Running --> Failed: lease expired, retry returns None or raises

    Running --> [*]: unreadable payload discarded

    note right of Successful
        Result stays until the result TTL.
    end note

    note right of Failed
        Result stays until the result TTL.
    end note
```

SUCCESSFUL is terminal for the task. A successful result stays in Redis for
the result TTL, so `get_result()` can read it. The task never returns to the
queue. The TTL reaper only removes the stored result.

FAILED is terminal for the worker.

A retry never enters FAILED. When the task raises, the worker calls the task's
retry callback. A returned `datetime.timedelta` moves the task to DEFERRED with
that delay. The broker moves it to READY when the delay elapses, and the next
lease increments `attempts`, which is `len(worker_ids)`. The callback reads the
count through `TaskContext.attempt`. `ExponentialBackoff` keeps returning a
delay until `max_retries`. A callback that returns `None` or raises ends
retrying, and the task is acknowledged as FAILED.

The Ready tab shows READY and DEFERRED tasks. Result payloads expire after the
result TTL. The SUCCESSFUL and FAILED sets are trimmed on later
acknowledgements.

## Task flow

The worker process leases ready tasks, runs them, and acknowledges the result.
The broker thread moves due deferred tasks and reaps expired leases. An expired
lease marks the task FAILED with an `AcknowledgementTimeoutError`. The retry
rule then decides between requeue and acknowledge.

```mermaid
sequenceDiagram
    autonumber
    actor Producer
    participant Worker as Worker process
    participant Backend as RedisTaskBackend
    participant Redis
    participant Broker as Broker thread

    Producer->>Backend: enqueue(task)
    Backend->>Redis: HSET payload and score
    alt run_after is set
        Backend->>Redis: ZADD deferred
    else immediate task
        Backend->>Redis: ZADD ready
    end
    Backend-)Redis: PUBLISH ingress

    loop Every broker interval
        Broker->>Redis: ZADD due deferred tasks to ready
        opt A running lease expired
            Broker->>Backend: get_leased_task(task_id)
            Broker->>Backend: retry_delay(failed result)
            alt retry delay returned
                Backend->>Redis: ZREM running, restore payload, ZADD deferred
            else retry returns None or raises
                Backend->>Redis: ZREM running, SET result, DEL payload, ZADD failed
            end
        end
    end

    loop Until the task succeeds, fails without a retry, or is discarded
        Worker->>Backend: acquire(queues, count, worker)
        Backend->>Redis: ZPOPMIN ready
        Backend->>Redis: ZADD running with lease deadline
        Backend->>Redis: HSET lease worker, start time, token
        Redis-->>Backend: task data and lease token
        Backend-->>Worker: task result with attempt count
        Worker->>Worker: call task function

        alt task returned
            Worker->>Backend: acknowledge(successful result)
            Backend->>Redis: ZREM running, SET result, DEL payload, ZADD successful
            Backend-)Redis: PUBLISH egress
        else task raised, retry delay returned
            Worker->>Backend: requeue(result, now + delay)
            Backend->>Redis: ZREM running, DEL result, restore payload, ZADD deferred
            Backend-)Redis: PUBLISH ingress
            Note over Worker,Redis: The broker returns the task to ready after the delay.
        else task raised, retry returns None or raises
            Worker->>Backend: acknowledge(failed result)
            Backend->>Redis: ZREM running, SET result, DEL payload, ZADD failed
            Backend-)Redis: PUBLISH egress
        end
    end
```

Acknowledge only removes the RUNNING entry when the lease token matches. A worker that lost its lease cannot publish a result. The broker then owns the retry decision for that task.

## Inspector actions

The inspector reads on its own and mutates only after an explicit
confirmation. Every mutating action refreshes telemetry and the task list.

```mermaid
sequenceDiagram
    autonumber
    actor Operator
    participant Inspector
    participant Backend as RedisTaskBackend
    participant Redis
    participant Broker as Broker thread

    loop Every 2 seconds
        Inspector->>Backend: queue_stats()
        Backend->>Redis: ZCARD all five segments
        Redis-->>Backend: counts
        Backend-->>Inspector: telemetry
    end

    Redis--)Inspector: ingress and egress events

    Operator->>Inspector: select queue and status tab
    Inspector->>Backend: peek(queue, status, FOREVER, 100)
    Backend->>Redis: read members and payloads
    Redis-->>Backend: stored task results
    Backend-->>Inspector: task results

    Operator->>Inspector: press r on the Failed tab
    Inspector->>Operator: ConfirmScreen
    Operator-->>Inspector: Enter
    Inspector->>Backend: requeue(task, now)
    Backend->>Redis: ZREM failed, DEL result, restore payload, ZADD deferred
    Note over Inspector,Redis: The retry callback is skipped.
    Broker->>Redis: move due task to ready on the next pass
    Inspector->>Inspector: refresh telemetry and task list

    Operator->>Inspector: press d on Ready, Successful, or Failed
    Inspector->>Operator: ConfirmScreen
    Operator-->>Inspector: Enter
    Inspector->>Backend: dequeue(task)
    Backend->>Redis: ZREM deferred or status segment, DEL payload and result
    Inspector->>Inspector: refresh telemetry and task list

    Operator->>Inspector: press p on a queue
    Inspector->>Operator: PurgeScreen asks for the queue name
    Operator-->>Inspector: queue name and Enter
    Inspector->>Backend: purge(queue)
    Backend->>Redis: delete five segments with payloads and results
    Inspector->>Inspector: refresh telemetry and task list
```

Requeue appears only on the Failed tab. Dequeue is hidden on the Running tab,
so the inspector never races a live lease. Purge deletes every segment of a
queue, including results. The task list does not auto-refresh. It reloads on
tab switch, queue change, F5, and after each action.

## Testing

We have unit tests, integration tests, and benchmarks. Avoid mocking if possible.

To run the tests, use the following command:

```bash
uv run pytest
```

To run only integration tests:

```bash
uv run pytest -m integration
```

To run only integration benchmarks:

```bash
uv run pytest -m "integration and benchmark"
```

Benchmarking snapshots are created automatically.
To compare your feature branch against the main branch,
run the test suite on main, followed by:

```
uv run pytest --benchmark-compare
```

Before your first commit, ensure that the pre-commit hooks are installed by running:

```bash
uvx prek install
```
