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
    [*] --> Deferred: enqueue with run_after
    [*] --> Ready: enqueue
    Deferred --> Ready: run_after reached (broker)
    Ready --> Running: acquired with a lease
    Running --> Successful: task returned
    Running --> Failed: task raised without a retry delay
    Running --> Deferred: task raised with a retry delay
    Running --> Deferred: lease expired with a retry delay
    Running --> Failed: lease expired without a retry delay
    Failed --> Deferred: requeued from the inspector
    Ready --> [*]: dequeued from the inspector
    Deferred --> [*]: dequeued from the inspector
    Successful --> [*]: dequeued from the inspector
    Failed --> [*]: dequeued from the inspector
    Running --> [*]: unreadable payload discarded
```

The Ready tab shows READY and DEFERRED tasks. Requeue and the worker retry
both store the task in DEFERRED. The broker moves it to READY when it is due.
Result payloads expire after the result TTL. The SUCCESSFUL and FAILED sets are
trimmed on later acknowledgements.

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
        Broker->>Redis: read due deferred tasks
        Broker->>Redis: ZADD ready (drop missing payloads)
    end

    Worker->>Backend: acquire(queues, count, worker)
    Backend->>Redis: ZPOPMIN ready
    Backend->>Redis: ZADD running with lease deadline
    Backend->>Redis: HSET lease worker, start time, token
    Redis-->>Backend: leased task data
    Backend-->>Worker: task results with lease tokens
    Worker->>Worker: call task function

    alt task returned
        Worker->>Backend: acknowledge(successful result)
        Backend->>Redis: ZREM running, SET result, DEL payload, ZADD successful
        Backend-)Redis: PUBLISH egress
    else task raised with a retry delay
        Worker->>Backend: requeue(result, run_after)
        Backend->>Redis: ZREM running, DEL result, restore payload, ZADD deferred
        Backend-)Redis: PUBLISH ingress
    else task raised without a retry delay
        Worker->>Backend: acknowledge(failed result)
        Backend->>Redis: ZREM running, SET result, DEL payload, ZADD failed
        Backend-)Redis: PUBLISH egress
    end
```

Acknowledge only removes the RUNNING entry when the lease token matches. A worker that lost its lease cannot publish a result.

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
