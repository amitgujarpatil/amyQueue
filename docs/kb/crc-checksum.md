# CRC Checksum in RecordBatch

## What CRC Is

CRC (Cyclic Redundancy Check) is a checksum — a mathematical fingerprint
of a block of data. Run the algorithm over bytes, get a fixed-size number.
If even one bit in the data changes, the output is completely different.

```
data = "hello world"  →  CRC32C = 0xC99465AA
data = "hello World"  →  CRC32C = 0x1234BEEF   ← 1 character changed, entirely different result
```

CRC is not encryption. It does not hide data. It only detects whether data
was corrupted between when it was written and when it is read.

---

## Why It Matters in a Message Queue

Data passes through multiple handoffs where silent corruption can happen:

```
Producer → [network] → Leader → [disk] → Follower → [network] → Consumer
```

At each arrow, something can silently go wrong:
- **Network**: a bit flips in transit (rare but real at scale)
- **Disk**: a sector goes bad, bit rot on a long-lived segment file
- **Partial write**: broker crashes mid-write, segment has half-written bytes

Without CRC, corrupt data would be passed along silently. The consumer gets
garbage bytes with no way to know. With CRC, corruption is caught at the
next handoff and surfaced as an error instead of silent data loss.

---

## How It Works at Each Handoff

**Producer writes a batch:**
```
1. Build RecordBatch (attributes, offsets, timestamps, records)
2. Compute CRC32C over those bytes
3. Store the 4-byte result in the crc field
4. Send to broker
```

**Broker receives from producer:**
```
1. Read RecordBatch from network
2. Recompute CRC32C over same bytes
3. Compare with stored crc field
4. Mismatch → reject, return error to producer
5. Match → write to disk
```

**Follower fetches from leader:**
```
1. Receive RecordBatch over network
2. Recompute CRC32C
3. Mismatch → do not write, ask leader to resend
4. Match → write to local segment
```

**Consumer reads:**
```
1. Receive RecordBatch
2. Recompute CRC32C
3. Mismatch → reject, surface error to application
4. Match → deliver to application
```

CRC is checked at every handoff. Corruption anywhere in the chain is
caught at the next check — never silently propagated.

---

## Concrete Example: Disk Bit Rot

```
t=0        Producer writes batch. CRC stored = 0xABCD1234.
           Follower replicates. Both disks have identical bytes.

t=100days  A sector on the follower disk flips a bit.
           The RecordBatch bytes now read differently.
           The stored CRC field is still 0xABCD1234 (unchanged on disk).

t=101days  Consumer fetches. Follower is now leader.
           Broker reads RecordBatch from disk.
           Recomputes CRC → 0xFF001234
           0xFF001234 != 0xABCD1234 → CORRUPTION DETECTED
           Broker returns error. Consumer is not served corrupt data.
```

Without CRC: consumer silently receives corrupted bytes.
With CRC: error is surfaced. Operator can investigate.

---

## Why CRC32C and Not SHA256

CRC is not a cryptographic hash. An attacker who can modify data can
also recompute a valid CRC for the tampered version. CRC only catches
accidental corruption — not intentional tampering.

CRC32C is chosen over SHA256 for speed:

```
SHA256:   ~500 MB/s   (software computation)
CRC32C:   ~10 GB/s    (hardware — Intel SSE4.2 has a native instruction)
```

A message queue processes GB/s of data. CRC32C runs at wire speed in
hardware. SHA256 at every RecordBatch would be the throughput bottleneck.
Accidental corruption is the real threat at this layer — CRC32C is the
correct tradeoff.

---

## What CRC Covers in RecordBatch

Not every field is included in the CRC computation:

```
RecordBatch:
  baseOffset       int64   ← excluded (framing — needed to locate batch before CRC is known)
  batchLength      int32   ← excluded (framing)
  magic            int8    ← excluded (framing)
  ──────────────────────────────────
  crc              int32   ← the checksum (covers everything below this line)
  ──────────────────────────────────
  attributes       int16   ← covered
  lastOffsetDelta  int32   ← covered
  baseTimestamp    int64   ← covered
  maxTimestamp     int64   ← covered
  producerID       int64   ← covered
  producerEpoch    int16   ← covered
  baseSequence     int32   ← covered
  records[]               ← covered (all message content)
```

The framing fields (baseOffset, batchLength, magic) are excluded because
the broker reads them first to find and size the batch in the file —
before it can validate the CRC. Everything that is actual message content
is always covered.

---

## What CRC Does NOT Catch

| Scenario | CRC catches it? |
|---|---|
| Single bit flip in message data | Yes |
| Partial write (crash mid-write) | Yes — incomplete batch has wrong CRC |
| Disk bit rot | Yes |
| Network bit flip | Yes |
| Intentional data tampering | No — attacker can recompute CRC |
| Complete data loss (file deleted) | No — CRC requires the data to be present |
| Wrong data served to wrong consumer | No — CRC verifies integrity, not authorization |

For intentional tamper detection: use TLS in transit (mTLS) and rely on
filesystem permissions + OS security on disk. CRC is purely an integrity
layer, not a security layer.
