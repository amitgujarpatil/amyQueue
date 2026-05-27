# Cluster Authentication: Design Discussion and Decisions

> Spans Phase 1 (Raft cluster join + ClusterID) and Phase 2 (broker registration).
> This doc is the single source of truth for cluster authentication design.

---

## The Problem

Without authentication any node that knows the controller address can:

```
1. Join the Raft cluster as an observer or voter
2. Register as a broker and receive full partition metadata
3. Accept produce requests and silently drop writes
4. Observe all cluster metadata including topic names and partition layout
```

Two separate attack surfaces must be locked down:

```
Surface 1 — Raft cluster join
  Any node can send ObserverJoin to the controller
  and become a member of the controller cluster

Surface 2 — Broker registration and ongoing calls
  Any process can POST /brokers/register
  and be treated as a legitimate broker
```

---

## What Kafka Does

Kafka uses two independent mechanisms:

**ClusterID** — a UUID generated on first cluster init, stored in the metadata
log. Every broker config must contain this ClusterID. On registration the broker
presents it. Wrong or missing ClusterID is rejected. This protects against
accidents (broker from cluster A joining cluster B) but not against a malicious
node that already knows the ClusterID.

**mTLS** — each node has a certificate signed by a shared cluster CA. On every
connection both sides present their cert. If not signed by the cluster CA the
connection is rejected before any data is exchanged. This is the actual security
layer. SASL is an alternative for simpler setups.

---

## Decision — ClusterID + Shared Cluster Token

Two layers:

**Layer 1 — ClusterID (sanity check, same as Kafka)**
Generated once on CmdClusterInit. Stored in the Raft log and in Store.ClusterID.
Never changes. All broker and controller configs must include it. Validated on
every registration and Raft join request.

**Layer 2 — Shared Cluster Token (authentication)**
A secret string configured on all trusted nodes. Sent on every inbound request.
Controller validates before touching any state. Supports safe zero-downtime
rotation via a two-token grace period.

mTLS is deferred — correct long-term direction but cert management complexity
is out of scope for now. The ClusterAuth interface is designed so a transport-
layer mTLS adapter can replace the application-layer token check later without
changing any application code.

---

## Configuration

Both config file and environment variable are supported.
Environment variable takes precedence over config file.
Default token is "amyqueue_token" — operators MUST change this in production.

```go
type ClusterAuthConfig struct {
    ClusterID             string // set once on init, read-only after
    ClusterToken          string // primary token
    ClusterTokenSecondary string // rotation grace period token, empty = disabled
}
```

Config file:
```yaml
cluster_id: "uuid-generated-on-init"
cluster_token: "amyqueue_token"
cluster_token_secondary: ""
```

Environment variables:
```
AMYQUEUE_CLUSTER_ID=...
AMYQUEUE_CLUSTER_TOKEN=amyqueue_token
AMYQUEUE_CLUSTER_TOKEN_SECONDARY=
```

---

## Token Rotation — Zero Downtime Procedure

The system accepts two tokens simultaneously during the rotation window.
Outgoing requests always use PrimaryToken.

```
BEFORE ROTATION
  All nodes: primary="old_secret"  secondary=""

STEP 1 — Add new token as secondary on ALL nodes
  All nodes: primary="old_secret"  secondary="new_secret"
  Effect: all nodes now accept both old and new tokens
  Risk: zero — no requests rejected during this step

STEP 2 — Promote new token to primary on ALL nodes
  All nodes: primary="new_secret"  secondary="old_secret"
  Effect: outgoing requests now use new token
          incoming old-token requests still accepted (secondary)
  Risk: zero — in-flight requests with old token still pass

STEP 3 — Clear secondary on ALL nodes
  All nodes: primary="new_secret"  secondary=""
  Effect: old token no longer accepted
  Risk: zero — all nodes already using new token by step 2

AFTER ROTATION
  Rotation complete. Old token fully retired.
```

Each step is a rolling config update — nodes do not need to restart
simultaneously. Apply step 1 to all nodes before moving to step 2.

---

## ClusterAuth Interface

Application layer now, designed so a transport-layer adapter (mTLS) can replace
it later without any application code changes.

```go
// ClusterAuth validates inbound cluster membership requests.
// Implemented as application-layer header check now.
// Can be replaced with a transport-layer TLS adapter in future.
type ClusterAuth interface {
    ValidateRequest(clusterID, token string) error
}

// TokenClusterAuth is the application-layer implementation.
type TokenClusterAuth struct {
    config ClusterAuthConfig
}

func (a *TokenClusterAuth) ValidateRequest(clusterID, token string) error {
    if clusterID != a.config.ClusterID {
        return ErrWrongCluster
    }
    if token == a.config.ClusterToken {
        return nil
    }
    if a.config.ClusterTokenSecondary != "" &&
        token == a.config.ClusterTokenSecondary {
        return nil  // grace period during rotation
    }
    return ErrUnauthorized
}
```

HTTP middleware calls `ClusterAuth.ValidateRequest` on every protected endpoint
before routing to the handler. The handler never sees unauthenticated requests.

**Why application layer now:** we do not have TLS infrastructure yet. An HTTP
header check is simple, correct, and immediately useful. The interface boundary
means the upgrade to transport-layer mTLS is a single adapter swap — no
application code changes.

---

## What Carries Auth on Each Request

| Request | ClusterID | Token | Epoch |
|---|---|---|---|
| Raft ObserverJoin | yes | yes | — |
| Raft AddVoter | yes | yes | — |
| POST /brokers/register | yes | yes | — |
| POST /brokers/{id}/heartbeat | yes | yes | yes |
| POST /brokers/{id}/leo-report (Phase 7) | yes | yes | yes |
| GET /cluster/info | — | — | — (public) |

Epoch is validated separately by the state machine after ClusterAuth passes.
ClusterAuth is the gate. Epoch is the fencing check inside the gate.

---

## ClusterID HTTP Endpoint

Operators need to verify which cluster they are talking to.

```
GET /cluster/info

Response 200:
{
  "clusterID":  "550e8400-e29b-41d4-a716-446655440000",
  "version":    42
}
```

This endpoint is public — no token required. ClusterID is not a secret.
It is a cluster identity, not a credential.

---

## Phase 1 Changes

**Store:**
```go
type Store struct {
    ...
    ClusterID string   // set once on CmdClusterInit, never changes
}
```

**CmdClusterInit log entry:**
Generated by the controller on fresh cluster startup. Idempotent — if ClusterID
is already set in the store, no-op. ClusterID is a UUID generated by the HTTP
handler before the log entry is written (same pattern as TopicID in D1 — UUID
generated at propose time, never inside Apply).

```go
type ClusterInitPayload struct {
    ClusterID string   // UUID generated at propose time
}
```

applyClusterInit:
```
if store.ClusterID != "" -> no-op (idempotent)
else: store.ClusterID = payload.ClusterID
```

**Raft join validation:**
ObserverJoin and AddVoter requests gain ClusterID and Token fields.
The Raft HTTP handler validates via ClusterAuth before forwarding to the node.

---

## Phase 2 Changes

**RegisterBrokerPayload gains auth fields:**
```go
type RegisterBrokerPayload struct {
    BrokerID  BrokerID
    Host      string
    Port      int32
    RackID    string
    ClusterID string   // must match store.ClusterID
    Token     string   // validated by ClusterAuth middleware
}
```

Token validation happens in the HTTP middleware — before the payload ever
reaches the state machine. The state machine only validates ClusterID (a
second check inside Apply as a defence-in-depth measure).

**Heartbeat and all subsequent broker calls:**
ClusterID + Token in every request header. Middleware validates. Stale epoch
is a separate check inside the handler after middleware passes.

---

## Error Responses

| Error | HTTP Status | Message |
|---|---|---|
| Missing token | 401 | "cluster token required" |
| Wrong token | 401 | "invalid cluster token" |
| Wrong ClusterID | 403 | "cluster ID mismatch" |
| Stale broker epoch | 403 | "stale broker epoch, re-register" |

401 for credential errors (token). 403 for identity errors (wrong cluster,
stale epoch). Clients can distinguish and react appropriately.

---

## Optional Mutual TLS (Phase 2)

mTLS is supported as an opt-in transport-layer security layer. Off by default. Enabled via config or env var. See full design → `phase2-final.md` D10.

**When disabled (default):** plain HTTP. Shared token is the only protection.

**When enabled:** HTTPS with mutual TLS. Controller and broker both present certificates signed by the cluster CA. Both sides verify the peer's cert. A node without a valid cert cannot complete the TLS handshake.

**Relationship with shared token when mTLS is on:**
The token check still runs at the application layer — defence-in-depth. mTLS adds transport encryption and mutual identity proof. The token adds application-layer authorization. They are complementary, not redundant.

**Future upgrade path:** A `TLSClusterAuth` adapter can replace `TokenClusterAuth` entirely once mTLS is the sole trust mechanism. The `ClusterAuth` interface makes this a single adapter swap — no application code changes.

**Summary of what each layer provides:**

| Layer | Mechanism | What It Provides |
|---|---|---|
| Transport (optional) | mTLS | Encryption + mutual identity via cert |
| Application (always) | ClusterID + Token | Cluster identity sanity + shared secret auth |

---

## What Is NOT Covered (Deferred)

| Feature | Why Deferred |
|---|---|
| Per-broker ACLs | Overkill — all cluster nodes are equally trusted |
| Client-facing auth (producers/consumers) | Separate concern — Phase 4+ |
| Token expiry / TTL | Tokens are long-lived secrets, not JWTs |
| Audit log of auth events | Useful but not critical for now |
| Certificate hot reload | Restart required for cert changes, hot reload deferred |
| Replacing token with TLSClusterAuth adapter | Deferred — token + mTLS run in parallel for now |

---

## Summary

| Question | Decision |
|---|---|
| Auth mechanism | ClusterID (sanity) + Shared Token (auth) |
| Token default | "amyqueue_token" |
| Token source | Config file AND env var — env var takes precedence |
| Token rotation | Two-token grace period — zero downtime three-step procedure |
| Where is auth enforced | Application-layer HTTP middleware (ClusterAuth interface) |
| Can it move to transport layer | Yes — ClusterAuth interface makes it a single adapter swap |
| ClusterID in HTTP API | Yes — GET /cluster/info (public, no token required) |
| mTLS | Optional — off by default, enabled via tls.enabled config / AMYQUEUE_TLS_ENABLED env var |
