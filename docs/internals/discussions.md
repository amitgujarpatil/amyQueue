# AmyQueue — Design Discussions

Every significant design conversation, the options considered, and the
decision made (or still pending). Updated as we talk.

---

## 2026-05-12 — Raft transport layer abstraction

**Question:** Where should TCP, gRPC, and HTTP code live? The broker and
controller both need transport but with different service contracts.

**Discussion:**
- Flat `internal/grpc` and `internal/http` dirs would mix Raft gRPC +
  broker gRPC + producer gRPC — same protocol, completely different contracts.
- Grouping by protocol meant the package would grow into an unrelated mess.

**Decision:** Group by domain, not protocol.
- `transport/tcp`, `transport/grpc`, `transport/http` — generic wire primitives,
  zero domain knowledge. The seam is `ConnHandler func(net.Conn)`.
- Domain packages (`raft/tcp`, `api/producer/tcp`) import transport primitives
  and own their own message framing. Transport never imports domain.
- Rule: dependency arrow always points domain → transport, never the reverse.

**Outcome:** `raft/tcp` now uses `transport/tcp.Server` and `transport/tcp.Dial`.
Adding gRPC for Raft = implement `raft.Transport` interface in `raft/grpc/`,
zero changes to Raft core.

---

## 2026-05-12 — Dynamic cluster membership: how Kafka handles it

**Question:** Our controllers are static today. How do we safely add/remove
controllers at runtime without restart, downtime, or split-brain?

**Discussion:**
- Naive approach (swap config live) is dangerous: two quorum definitions can
  be active simultaneously → two leaders possible → split-brain.
- Kafka KRaft stores the voter set inside the Raft log itself. A membership
  change is a special log entry committed by the OLD quorum before the new
  quorum takes effect. Zero window where two quorums coexist.
- Two standard approaches: single-server changes (Kafka's choice, simpler,
  proven safe mathematically) vs joint consensus (Raft paper, allows bulk
  changes, much harder to implement).

**Decision:** Single-server changes only (one add or remove per operation).
Membership changes go through the Raft log as `CmdMembership` entries.
New nodes always start as observers — they replicate the log but don't vote
until explicitly promoted.

**Modes:**
- `CLUSTER_MODE=static` — voter set fixed from `PEER_NODES` at startup.
- `CLUSTER_MODE=dynamic` — voter set managed via Raft log entries.

**Outcome:** `VoterSet` added, replaces static peers slice. `MembershipChange`
log entries apply on commit. Admin HTTP API for add/remove voter.

---

## 2026-05-13 — Leader discovery for new nodes joining dynamically

**Question:** A new node only knows bootstrap seed addresses, not who the
current leader is. ObserverJoin must go to the leader. How does it find it?

**Discussion:**
- Option 1: Separate `ClusterInfo` RPC first (ask any node → get leader addr),
  then `ObserverJoin` to leader. Two round-trips even if first seed is leader.
- Option 2: Send `ObserverJoin` directly to each seed. Non-leaders return
  `LeaderAddr` in the response. Redirect immediately. One round-trip if seed
  is already the leader, two if not.
- Root issue: followers didn't store `leaderAddr`, only `leaderID`. Redirect
  hints were useless without an actual dial address.

**Decision:** Option 2. `ObserverJoin` itself handles redirect.
- `LeaderAddr` added to `AppendEntriesRequest` — leader self-reports its
  address in every heartbeat so all followers always know it.
- `BOOTSTRAP_SERVERS` replaces `JOIN_ADDR` (comma-separated, multiple seeds).
- Full retry loop: dead seed → skip, no leader yet → wait, redirect → follow.
- `JOIN_MAX_RETRIES` and `JOIN_RETRY_INTERVAL_MS` make it configurable.
- `ClusterInfo` kept as a handler for monitoring/HTTP status, removed from join path.

**Outcome:** Join flow is one function, no separate discovery step.
Any seed is a valid entry point. Failures are logged with exact reason.

---

## 2026-05-14 — Should observer-to-voter promotion be automatic?

**Question:** Currently an admin must call `POST /cluster/voters` to promote
an observer. Should this be automatic once the observer catches up?

**Discussion:**

Kafka KRaft keeps promotion as an explicit admin operation. Reasons:
- "Caught up" is necessary but not sufficient — admin decides intent.
- A monitoring-only observer should never become a voter.
- Premature auto-promotion of a barely-caught-up node can break quorum
  if that node crashes shortly after.
- Silent auto-promotion makes cluster membership opaque in production.

Risk of full auto-promotion:
- Accidental quorum growth (3-node → 4-node silently, now need 3 votes
  instead of 2, one failure breaks the cluster).
- No operator visibility into who is actually voting.

Two options considered:

**Option A — Auto-promote with opt-in flag (recommended)**
Node starts with `AUTO_PROMOTE=true`. Leader checks observer's `matchIndex`
against `lastIndex - LAG_THRESHOLD` on each heartbeat cycle. When caught up
and flag is set → leader triggers AddVoter automatically.
- Intent declared at startup, no silent surprises.
- Configurable lag threshold.
- `AUTO_PROMOTE=false` for read-only observers.

**Option B — Manual with caught-up validation (Kafka's approach)**
Admin calls `POST /cluster/voters`. Leader checks the observer is caught up
before writing the log entry. Rejects with "observer not ready, lag = N" if
not. Admin retries when ready.

**Decision pending.** Leaning toward Option A (`AUTO_PROMOTE` flag) because
it fits the dynamic intent while still keeping explicit control at startup.
Next step: implement Option A with `AUTO_PROMOTE` and `LAG_THRESHOLD` env vars.
