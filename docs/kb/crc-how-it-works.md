# CRC — How It Works When Both Data and CRC Can Be Corrupted

## The Question

CRC and the data it checksums travel together in the same RecordBatch on
the same network wire. If bits can flip in transit, how does reading the
(possibly corrupt) CRC and comparing it with a recomputed CRC actually
catch anything?

---

## What You Actually Do on Receipt

```
Received bytes: [ ... crc=0xABCD1234 ... data bytes ... ]
                        ↑                      ↑
                 may be corrupted         may be corrupted

Step 1: read the stored crc field from received bytes
Step 2: recompute CRC32C over the received data bytes
Step 3: compare the two values
```

Both values come from the same received byte stream. You do not have a
"clean" copy of either. You compare received-CRC against recomputed-CRC.

---

## The Three Scenarios

### Only data is corrupted — CRC field arrived intact

```
Transmitted:  crc=0xABCD1234  data=[intact]
Received:     crc=0xABCD1234  data=[1 bit flipped]

Recompute CRC over received data  →  0xFF001234
Compare:  0xFF001234 != 0xABCD1234  →  DETECTED ✓
```

### Only the CRC field is corrupted — data arrived intact

```
Transmitted:  crc=0xABCD1234  data=[intact]
Received:     crc=0xZZZZZZZZ  data=[intact]

Recompute CRC over received data  →  0xABCD1234
Compare:  0xABCD1234 != 0xZZZZZZZZ  →  DETECTED ✓
```

### Both CRC field and data are corrupted

```
Transmitted:  crc=0xABCD1234  data=[intact]
Received:     crc=0xCCCCCCCC  data=[bit flipped]

Recompute CRC over corrupted data  →  some value X
Compare:  X != 0xCCCCCCCC  →  almost certainly DETECTED ✓
```

---

## Why "Almost Certainly"?

The only undetectable case is if the corrupted data happens to produce
exactly the corrupted CRC value. Two independent random corruptions would
need to cancel each other out perfectly.

CRC32C produces a 32-bit number. The chance that randomly corrupted data
produces a specific 32-bit target value is:

```
1 / 2^32  =  1 in 4,294,967,296  ≈  0.000000023%
```

CRC32C is a polynomial specifically designed to minimise this. Its
detection guarantees:

| Error type | Detection rate |
|---|---|
| Single-bit errors | 100% |
| Double-bit errors | 100% |
| Any odd number of bit errors | 100% |
| Burst errors up to 32 bits | 100% |
| Larger random errors | 99.9999999% |

---

## The Right Mental Model

Think of a physical package with a wax seal. Both travel together.
If the package gets randomly damaged in transit, the seal almost certainly
will not still match the damaged contents.

The only way a corrupted package has a matching seal is if someone
intentionally forged both — coordinated, not random. This is why CRC
catches accidental corruption (random bit flips) but not intentional
tampering (attacker who can recompute the CRC for tampered data).

```
Random corruption → two independent random events would need to cancel
                    out in exactly the right way → probability 1/2^32

Intentional tampering → attacker computes a valid CRC for tampered data
                        → CRC gives zero protection
```

---

## Practical Implication

At 1 million RecordBatches per second, and assuming corruption hit every
single batch (it does not — corruption is rare):

```
Expected undetected corruption:  once every ~4,295 seconds (~71 minutes)
```

In practice corruption is rare — hardware errors on healthy disks are
measured in events per terabyte, not per batch. The effective miss rate
is negligible. This is the accepted engineering floor for storage systems.
ECC RAM has a similar residual error rate.

---

## Summary

CRC does not need a clean copy. It works because:

1. If only data is corrupted → recomputed CRC ≠ stored CRC → detected
2. If only CRC field is corrupted → recomputed CRC ≠ stored CRC → detected
3. If both are corrupted → two independent random corruptions would need
   to cancel perfectly → probability 1/4,294,967,296 → effectively detected

The check is not "are these bytes perfect?" — it is "is it statistically
plausible that these bytes arrived intact?" The answer is yes with
99.9999999% confidence, which is sufficient for a storage system.
