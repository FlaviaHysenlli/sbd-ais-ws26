# Exercise 6: Card Sentinel

## 1. Decision: how do the payments reach the models?

### Comparison of the four options

| Option | R1: payment path untouched | R2: every payment, every model | Cost to the database |
|---|---|---|---|
| 1. Polling | Violated: every agent adds queries to the system of record | Violated: `id > last_id` misses rows that commit out of id order (Exercise 5) | Query load for every reader |
| 2. Dual write | Violated: the certified payment code must change | Violated: a crash between commit and send loses the event, a send before the commit invents a rolled-back payment | None, but the application changes |
| 3. Outbox | Violated: the payment code must also write the outbox row | Met: both rows are in one transaction | One extra row per transaction |
| 4. Log-based CDC | Met: no code change, no queries | Met: every commit, in commit order | A replication slot: PostgreSQL keeps the WAL the connector has not read |

### Decision

I choose log-based CDC (option 4): Debezium reads PostgreSQL's write-ahead log and publishes every committed payment to Kafka in commit order. It is the only option that leaves the certified payment path unchanged and adds no query load (R1) while still delivering every committed payment (R2). Polling fails both: every agent adds queries to the system of record, and `WHERE id > last_id` misses rows that commit out of order (Exercise 5). Dual write needs a change to the payment code and can lose an event after a crash or invent a payment that was rolled back. The cost of CDC is the replication slot: PostgreSQL keeps all WAL the connector has not read, so if the connector stops for a weekend the disk can fill and the database, and with it the payment path, stops. In production I would monitor the WAL retained by the slot, the connector and task status, and the consumer lag, and alert before the disk is at risk. If my team owned the payment path, I would choose the outbox pattern (payment and outbox row in one transaction), which is just as correct and avoids the slot risk.

### Guide check (CDC with Debezium)

Connector status: both the connector and its task were `RUNNING`. For the table `activity`, an INSERT, UPDATE and DELETE became events with `op` = `c`, `u`, `d`, followed by a tombstone (key with value `null`). The key was the primary key (`{"id":1}`), chosen by Debezium without being asked. `before` of the update was `null` (PostgreSQL's default REPLICA IDENTITY logs only the old key).

## 2. The three runs

Routine for every run: `Ctrl+C` on agents and card processor, `python connector.py fraud-connector.json`, `python card_processor.py`, `python agents.py`, wait, `python scoreboard.py`.

### Run 1: Debezium's defaults

**Prediction:** the three velocity instances do not share the work and R4 fails. The connector file sets no partitions, so the topic gets the broker default (expected: 1 partition), and one instance cannot handle 1,000 payments/s.

**Observation**

Agents:

```
[ano-1] assigned partitions [0]
[vel-3] assigned partitions []
[vel-1] assigned partitions [0]
[vel-2] assigned partitions []
[vel-1]       393 transactions      0 alerts      393 cardholders  lag   25.2 s  out-of-order 0
[vel-1]    19,321 transactions      0 alerts   17,538 cardholders  lag   88.4 s  out-of-order 41
```

Kafka UI: `fraud.public.transactions` has **1 partition**. Key of an event: `"payload": {"id": 177075}` (the payment id, the primary key).

Scoreboard (velocity model):

```
velocity-agent   fraud.public.transactions   partitions 1   members 3   lag 23,642
vel-1   velocity   24,182 transactions   out-of-order 41   lag 109.0 s

R3  frauds caught          1/7  (14%)   FAIL  (>= 90 %)
R3  false alerts           0   PASS
R4  alert latency p50/p95  107.7 s / 108.9 s   FAIL  (p95 <= 5 s after the commit)
```

**Why:** nobody told Debezium how many partitions to use, so the topic was created with 1 partition. In a consumer group each partition is read by exactly one instance, so only `vel-1` worked and `vel-2` and `vel-3` were idle (`assigned partitions []`). One velocity instance handles only about 400 to 550 payments/s, so its lag grew every second (25 s to 109 s), R4 failed, and the most recent frauds were still waiting in the backlog (R3). The anomaly model should pass with one instance (0.3 ms per payment); in my run it also lagged, probably because my laptop was overloaded (not verified).

### Run 2: 6 partitions

**Change in `fraud-connector.json`:**

```json
"topic.creation.default.partitions": "6",
"topic.creation.default.replication.factor": "1",
```

**Prediction:** all three instances work (2 partitions each) and the lag drops, so R4 passes. But velocity R3 still fails, because the key is the payment id: one cardholder's payments are spread over different partitions and instances.

**Observation**

Connector (old connector, slot and topic removed, new topic created):

```
removing what writes to fraud.* ...
  deleted connector fraud-connector-123139
  dropped replication slot fraudslot
  deleted topic fraud.public.transactions
registered fraud-connector-124840
connector fraud-connector-124840: RUNNING
```

Agents:

```
[vel-1] assigned partitions [0, 1]
[vel-2] assigned partitions [2, 3]
[vel-3] assigned partitions [4, 5]
[ano-1] assigned partitions [0, 1, 2, 3, 4, 5]
```

Kafka UI: 6 partitions, filled evenly (32,061 to 32,560 messages each). Key of an event is still `{"id": ...}`; the cardholder `user_id` is only a normal field in the value.

Scoreboard (velocity model, after the run):

```
vel-1   velocity   65,666 transactions   out-of-order 24   lag 166.1 s
vel-2   velocity   66,261 transactions   out-of-order 37   lag 165.4 s
vel-3   velocity   65,943 transactions   out-of-order 29   lag 166.9 s

R3  frauds caught          0/22  (0%)   FAIL  (>= 90 %)
R3  false alerts           0   PASS
R4  alert latency p50/p95  78.8 s / 78.8 s   FAIL  (p95 <= 5 s after the commit)
```

**Why:** every instance now has two partitions, so nobody is idle. But the partition is `hash({"id": ...}) mod 6`, so the 12 test payments of one stolen card land on random partitions and are spread over all three instances. Each sees only about 4 of them in its 60 s window, below the threshold of 8, so no instance sees the whole attack. The anomaly model would also fail with two instances; with one instance it reads all six partitions. My R4 prediction was wrong on my laptop: each agent handled only about 200 payments/s and the lag grew to over 160 s (probably an overloaded machine, not verified), so the R3 count is also affected by the backlog.

### Run 3: the cardholder as key

**Change in `fraud-connector.json`** (in addition to Run 2; the card processor was started with `--tps 500` to relieve my laptop):

```json
"message.key.columns": "public.transactions:user_id",
```

**Prediction:** the key becomes `{"user_id": ...}`, all payments of a cardholder land in one partition and one instance, so velocity R3 passes. R4 passes if my computer keeps up. I also expected the partitions to become uneven.

**Observation**

Key of an event (Kafka UI):

```
"payload": {"user_id": 159607}
```

Kafka UI, messages per partition: 15,493 / 15,521 / 15,440 / 15,252 / 15,213 / 15,556 (total 92,475): evenly filled.

Scoreboard:

```
Run 1795003305: started 13:01:45, 50,106 transactions in 100 s (501/s), 9 frauds so far (still running)

  vel-1   velocity   14,701   out-of-order  9   lag 4.3 s
  vel-2   velocity   15,172   out-of-order  8   lag 3.9 s
  vel-3   velocity   14,517   out-of-order 11   lag 4.2 s

  velocity model (card testing)
    R3  frauds caught          4/4  (100%)   PASS  (>= 90 %)
    R3  false alerts           0   PASS
    R4  alert latency p50/p95  3.7 s / 6.6 s   FAIL  (p95 <= 5 s after the commit)

  anomaly model (account takeover)
    R3  frauds caught          4/4  (100%)   PASS
    R4  alert latency p50/p95  3.5 s / 5.3 s   FAIL
```

**Why:** the key is now the cardholder, so all payments of one cardholder go to the same partition and are read by one instance, in commit order. That instance counts all 12 test payments and flags the 8th: velocity R3 went from 0/22 to 4/4, with no false alerts. R4 failed narrowly: the lag was stable at about 4 s, so the pipeline keeps up, but it has no headroom on my laptop, where PostgreSQL, Kafka, Connect, the Kafka UI, the card processor and four agents all run together (exact cause not verified). Fixes suggested by the assignment: a fourth velocity instance or a lower rate. The partitions stayed even, so my prediction of an uneven load was wrong: with 100,000 cardholders the hash spreads the keys evenly (not verified against `card_processor.py`).

### Summary of the three runs

| | Run 1: defaults | Run 2: 6 partitions | Run 3: key = cardholder |
|---|---|---|---|
| Partitions | 1 | 6 | 6 |
| Key | `{"id": ...}` | `{"id": ...}` | `{"user_id": ...}` |
| Velocity instances working | 1 of 3 | 3 of 3 | 3 of 3 |
| Velocity R3 | 1/7 (14%) FAIL | 0/22 (0%) FAIL | 4/4 (100%) PASS |
| Velocity R4 (p95) | 108.9 s FAIL | 78.8 s FAIL | 6.6 s FAIL (narrow) |
| Agent lag | 25 s to 109 s, growing | 15 s to 166 s, growing | about 4 s, stable |
| Card processor rate | about 667/s | about 950/s | 500/s (`--tps 500`) |

Runs 1 and 2 were measured while the agents were far behind, so their R3 values are partly affected by the backlog. Run 3 is the only run where the agents kept up.

## 3. Closing answer: the two defaults

Debezium chose 1 partition and the primary key as the message key. Both are right for copying a table and wrong for a stateful model that has to scale. If nobody had checked them, nothing would have crashed and no error would have appeared: in production, the velocity model would have caught almost none of the card-testing frauds, and the alerts would have come minutes late, while the system looked healthy. The team could have noticed with a scoreboard-like check: measure the detection rate and alert latency against confirmed frauds, and monitor the consumer group (idle instances with `assigned partitions []`, consumer lag growing). They should also review topic settings (partitions, key) before go-live, because the key must be the entity whose state the model keeps.

A threshold rule (flag the 8th payment within 60 s) also cannot tell a thief from an honest cardholder who pays 8 times in a minute. That is the price of any threshold, and R3 allows a few false alerts. I saw none in my runs.

## 4. Final connector

Also in `fraud-connector.json` in this folder.

```json
{
  "name": "fraud-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgrespw",
    "database.dbname": "mydb",
    "topic.prefix": "fraud",
    "plugin.name": "pgoutput",
    "decimal.handling.mode": "double",
    "table.include.list": "public.transactions",
    "topic.creation.default.partitions": "6",
    "topic.creation.default.replication.factor": "1",
    "message.key.columns": "public.transactions:user_id",
    "slot.name": "fraudslot"
  }
}
```
