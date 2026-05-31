# Write Durability — acks + MinISR

## What Controls Durability

Two independent knobs. Both must be understood together.

| Knob | Who Sets It | What It Controls |
|---|---|---|
| `acks` | Producer, per request | When the producer considers a write successful |
| `MinISR` | Operator, per topic | Minimum live replicas required to accept writes at all |

---

## acks — Producer's Durability Choice

### acks=0 — Fire and Forget

```
Producer → sends batch → does not wait for any response
Success returned immediately (before the broker even receives it)

Guarantee: none
Use case: metrics, logs where occasional loss is acceptable
```

### acks=1 — Leader Only

```
Producer → sends batch → waits for leader to write to local log
Leader writes to disk → returns SUCCESS
Followers may or may not have replicated yet

Guarantee: leader received it
Risk: if leader dies before followers replicate → DATA LOSS
      producer received SUCCESS, data is gone
```

### acks=all (acks=-1) — All ISR Members

```
Producer → sends batch → waits for leader
Leader writes to disk
Leader waits for all current ISR members to fetch and acknowledge
HWM advances past the written offset
Leader returns SUCCESS

Guarantee: every current ISR member has the data
Risk: none beyond ISR failures (covered by MinISR)
```

---

## MinISR — The Operator's Safety Floor

MinISR is a write refusal floor. Set per topic in `TopicConfig.MinISR`.

```
If len(ISR) < MinISR:
  Refuse produce requests with acks=all
  Return NOT_ENOUGH_REPLICAS to producer
```

MinISR does NOT affect acks=0 or acks=1.
MinISR does NOT take the partition offline — reads still work.
MinISR only blocks writes when the cluster is genuinely under-replicated.

---

## The Combined Guarantee

```
acks=all + MinISR=2, RF=3:

  Normal state (ISR=[B0,B1,B2]):
    Write succeeds only after B0, B1, B2 all have it
    Any SUCCESS write is on 3 brokers
    Can lose 2 brokers without data loss

  After B0 dies (ISR=[B1,B2]):
    B1 elected leader
    Write succeeds only after B1, B2 both have it
    len(ISR)=2 >= MinISR=2 → writes continue
    Can still lose 1 more broker without data loss

  After B2 also dies (ISR=[B1]):
    len(ISR)=1 < MinISR=2 → writes REFUSED
    Reads still work from B1
    System waits for B2 to return and catch up before accepting writes
```

### Guarantee in One Line

```
acks=all + MinISR=2:
  Any acknowledged write is on ≥ 2 brokers.
  You can lose any 1 broker without losing that data.

acks=1:
  The leader received it. No further guarantee.
```

---

## Produce Validation Order on Broker

When a broker receives a ProduceRequest it must check in this order:

```
1. Check this broker is the current leader → LEADER_NOT_AVAILABLE if not
2. Check len(ISR) >= MinISR → NOT_ENOUGH_REPLICAS if not  (only for acks=all)
3. Write batch to local log segment
4. If acks=all: block until HWM advances past written offset
5. If acks=1:   return SUCCESS after step 3
6. If acks=0:   return SUCCESS before step 3 (or do not wait)
```

Step 2 runs before step 3. A write is never partially accepted.

---

## Common Misconceptions

**"MinISR takes the partition offline."**
No. The partition stays Online. Only writes with acks=all are refused.
Reads are never blocked by MinISR.

**"acks=1 is safe if MinISR=2."**
No. MinISR only affects acks=all. With acks=1 the producer does not wait
for any follower regardless of MinISR.

**"acks=all with RF=1 is the same as acks=1."**
Yes — with only one replica, ISR=[leader], acks=all waits for only the
leader. Same behaviour as acks=1 in this case.
