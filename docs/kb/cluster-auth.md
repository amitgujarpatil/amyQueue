# Cluster Authentication

## The Problem

Without authentication any node that knows the controller address can:

```
1. Join the Raft cluster and become a controller member
2. Register as a broker and receive full partition metadata
3. Accept produce requests and silently drop writes
4. Observe all cluster metadata: topic names, partition layout, broker addresses
```

Two attack surfaces must be protected:
- Raft cluster join (ObserverJoin, AddVoter)
- Broker registration and all ongoing broker requests

---

## Two-Layer Design

### Layer 1 — ClusterID (Sanity Check)

Generated once on `CmdClusterInit`. Stored in the Raft log and in `Store.ClusterID`.
Never changes. All broker and controller configs must include it.

Protects against accidents — a broker from cluster A accidentally connecting to
cluster B. Does NOT protect against a malicious node that already knows the ClusterID.

`ClusterID` is not a secret. `GET /cluster/info` is a public endpoint.

### Layer 2 — Shared Cluster Token (Authentication)

A secret string configured on all trusted nodes. Sent on every inbound request.
Validated by HTTP middleware before any handler is reached.

This is the actual security layer. A node without the correct token is rejected
before it can read any cluster state.

---

## ClusterAuth Interface

```go
type ClusterAuth interface {
    ValidateRequest(clusterID, token string) error
}

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
        return nil   // grace period during rotation
    }
    return ErrUnauthorized
}
```

The interface boundary means a `TLSClusterAuth` adapter can replace
`TokenClusterAuth` in future — no application code changes.

---

## Configuration

```yaml
cluster_id:               "uuid-generated-on-init"
cluster_token:            "amyqueue_token"     # CHANGE IN PRODUCTION
cluster_token_secondary:  ""                   # for rotation only
```

Environment variables (take precedence over config file):
```
AMYQUEUE_CLUSTER_ID=...
AMYQUEUE_CLUSTER_TOKEN=amyqueue_token
AMYQUEUE_CLUSTER_TOKEN_SECONDARY=
```

**Default token is `amyqueue_token`. Operators MUST change this in production.**

---

## Zero-Downtime Token Rotation

The system accepts two tokens simultaneously during the rotation window.
Outgoing requests always use the primary token.

```
STEP 1 — Add new token as secondary on ALL nodes
  All nodes: primary="old"  secondary="new"
  Effect:    both tokens accepted, no requests rejected

STEP 2 — Promote new token to primary on ALL nodes
  All nodes: primary="new"  secondary="old"
  Effect:    outgoing requests use new token, old still accepted

STEP 3 — Clear secondary on ALL nodes
  All nodes: primary="new"  secondary=""
  Effect:    old token fully retired
```

Apply each step to all nodes before moving to the next step.
No restarts required.

---

## Optional Mutual TLS

mTLS adds transport-layer encryption and mutual identity verification.
Off by default. Enabled via config/env.

```yaml
tls:
  enabled:   false
  cert_file: "/etc/amyqueue/tls/cert.pem"
  key_file:  "/etc/amyqueue/tls/key.pem"
  ca_file:   "/etc/amyqueue/tls/ca.pem"
```

```
AMYQUEUE_TLS_ENABLED=false
AMYQUEUE_TLS_CERT_FILE=
AMYQUEUE_TLS_KEY_FILE=
AMYQUEUE_TLS_CA_FILE=
```

When enabled: both controller and broker present certs signed by the cluster CA.
Both sides verify the peer's cert. A node without a valid cert fails the TLS
handshake before any HTTP data is exchanged.

The shared token check still runs when mTLS is enabled — defence-in-depth.

---

## What Each Request Must Carry

| Request | ClusterID | Token | Epoch |
|---|---|---|---|
| Raft ObserverJoin / AddVoter | yes | yes | — |
| POST /brokers/register | yes | yes | — |
| POST /brokers/{id}/heartbeat | yes | yes | yes |
| POST /brokers/{id}/shutdown | yes | yes | yes |
| GET /cluster/info | — | — | — (public) |
| GET /brokers | yes | yes | — |

---

## Error Responses

| Error | HTTP Status | Message |
|---|---|---|
| Missing token | 401 | "cluster token required" |
| Wrong token | 401 | "invalid cluster token" |
| Wrong ClusterID | 403 | "cluster ID mismatch" |
| Stale broker epoch | 403 | "stale broker epoch, re-register" |
