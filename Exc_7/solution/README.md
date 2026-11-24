# Exercise 7: From the log to the lake

## 1. The example: why does the checker fail?

The example job calls `read_new(None)`, which always reads the topic from the beginning, and writes straight into the lake. It never remembers where it stopped.

- First run: all 2,000,000 records are in the lake, 0 duplicates, `Exactly once: PASS`.
- Second run: the job copies the whole topic again, so the lake holds 4,000,000 records, **2,000,000 duplicates**, and the checker says `FAIL`. Nothing is missing, because everything is read again.

```
partition          topic offsets     in lake  duplicates    missing  not loaded
        0           [0, 464,145)     928,290     464,145          0           0
        1           [0, 493,340)     986,680     493,340          0           0
        2           [0, 516,274)   1,032,548     516,274          0           0
        3           [0, 526,241)   1,052,482     526,241          0           0
    total      2,000,000 records   4,000,000   2,000,000          0           0

Exactly once: FAIL — 2,000,000 duplicates
```

## 2. My decision: architecture B (the lake is the truth)

A run does two things that cannot happen at the same instant: write the data and remember the position. A crash between them is the problem.

| Architecture | What happens on a crash | Assessment |
|---|---|---|
| **A: state file** | Writing straight into the lake and saving the state afterwards are two separate steps. A crash while writing leaves some files in the lake with an old state, so the next run writes them again (duplicates). Saving the state first instead would lose records. | By design: duplicates or loss. *(I did not run A, so this is reasoning, not a measurement.)* |
| **B: lake is the truth** | The job writes into `STAGING` and `publish` moves the finished run into the lake with one rename. A crash leaves only unpublished files in `STAGING`. The position is derived from the lake itself, so there is no second place that can disagree with the data. | Chosen. |
| **C: dedup when reading** | Duplicates may reach the lake. Readers use `DEDUP_SQL`, which keeps one row per Kafka partition and offset. | Works for readers, but the lake itself contains duplicates and every reader must use the view. |

I chose B because a run is either completely visible or not visible at all, and the position cannot drift away from the data, because it is read from the data. The cost is that `lake_position()` must read the lake (the highest `kafka_offset + 1` per partition) at the start of every run. I did not measure how this scales for a much larger lake.

```python
def run_once():
    start = lake_position()               # 1. where did the last run stop? (ask the lake)
    new = read_new(start)                 # 2. read what is new
    run = write_files(new, STAGING)       # 3. write it to the staging folder, not to the lake
    publish(run)                          # 4. one rename into the lake (the lake is the memory)
```

## 3. Explanation of the result

- **Situations 1 to 3:** the first run loads 2,000,000 records. After 100,000 new records the next run loads exactly those, and a run with nothing new changes nothing (it only took 1.5 s).
- **Situation 4 (stop while writing):** some partition files had already been written to `STAGING`, but the run never reached `publish`. Readers see no change (2,100,000 records, 0 duplicates, 0 missing). The "not loaded" column shows the 100,000 new records that are waiting in the topic. The next run starts from the same position, discards the leftovers in `STAGING`, and loads them: 2,200,000 records, 0 duplicates.
- **Situation 5 (stop after writing):** the complete run is in `STAGING` but not published, so the situation is the same. The next run loads it: 2,300,000 records, 0 duplicates.
- **Why no record is missing or twice:** the lake only ever contains complete runs (one rename), and the next run starts at the highest offset found in the lake. A run that dies before `publish` leaves no trace in the lake, so it is simply redone.
- **Timing:** the first run took 128 s (2,000,000 records, probably including Spark's startup, not measured separately), later runs with 100,000 records took about 28 s.

## 4. Output of `check_all(run_once)`

```
situation                                 in lake  duplicates  missing  not loaded    run  check
-----------------------------------------------------------------------------------------------
1 first run                             2,000,000           0        0           0 128.4s  PASS
2 new records arrive, run again         2,100,000           0        0           0  27.8s  PASS
3 nothing new, run again                2,100,000           0        0           0   1.5s  PASS
4 run stops WHILE writing: right after  2,100,000           0        0     100,000         PASS
  ... then the next run                 2,200,000           0        0           0  28.1s  PASS
5 run stops AFTER writing: right after  2,200,000           0        0     100,000         PASS
  ... then the next run                 2,300,000           0        0           0  29.7s  PASS
-----------------------------------------------------------------------------------------------
Exactly once after every run: YES
```
