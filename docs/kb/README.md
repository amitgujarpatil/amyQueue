# AmyQueue Knowledge Base

Concept-level reference docs. Each file explains one concept — what it is,
why it exists, how it works, and how AmyQueue implements it.

These are not phase docs. They are stable references you can read independently
of the design discussion trail.

---

## Index

| File | Concept |
|---|---|
| [log-retention.md](log-retention.md) | Size and time based log retention, segment deletion, LSO |
| [high-watermark.md](high-watermark.md) | HWM — the commit line, consumer read fence, advancement mechanics |
| [isr.md](isr.md) | In-Sync Replicas — what ISR is, shrink, expand, MinISR |
| [epoch-fencing.md](epoch-fencing.md) | BrokerEpoch and PartitionEpoch — zombie prevention and CAS fencing |
| [write-durability.md](write-durability.md) | acks=0/1/all + MinISR — what durability guarantees mean |
| [controlled-shutdown.md](controlled-shutdown.md) | Graceful broker shutdown vs crash, the 30s problem |
| [cluster-auth.md](cluster-auth.md) | ClusterID + shared token + optional mTLS |
| [log-structure.md](log-structure.md) | Segment files, offsets, index files, active vs sealed segments |
