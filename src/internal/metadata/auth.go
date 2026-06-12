package metadata

import "errors"

// ErrInvalidToken is returned when the token doesn't match primary or secondary.
var ErrInvalidToken = errors.New("invalid cluster token")

// ErrWrongClusterID is returned when the clusterID in the request doesn't match.
var ErrWrongClusterID = errors.New("wrong cluster ID")

// ClusterAuth validates that an incoming request belongs to this cluster.
// Applied by HTTP middleware on every protected endpoint; handlers never see
// unauthenticated requests.
//
// To replace token-based auth with mTLS: implement this interface in a
// CertClusterAuth that inspects the TLS peer certificate — no middleware changes.
type ClusterAuth interface {
	ValidateRequest(clusterID, token string) error
}

// ClusterAuthConfig holds the credentials for TokenClusterAuth.
// PrimaryToken and SecondaryToken are loaded from env vars:
//
//	AMYQUEUE_CLUSTER_TOKEN          — primary (required)
//	AMYQUEUE_CLUSTER_TOKEN_SECONDARY — secondary (empty = disabled)
//
// Zero-downtime rotation procedure:
//
//	Step 1: set new token as SecondaryToken on ALL nodes  (both accepted)
//	Step 2: promote new token to PrimaryToken on ALL nodes (old still accepted)
//	Step 3: clear SecondaryToken on ALL nodes              (old fully retired)
type ClusterAuthConfig struct {
	ClusterID      string
	PrimaryToken   string
	SecondaryToken string // empty = disabled
}

// TokenClusterAuth validates requests using a shared cluster token.
// Both PrimaryToken and SecondaryToken are accepted to enable zero-downtime rotation.
type TokenClusterAuth struct {
	cfg ClusterAuthConfig
}

func NewTokenClusterAuth(cfg ClusterAuthConfig) *TokenClusterAuth {
	return &TokenClusterAuth{cfg: cfg}
}

func (a *TokenClusterAuth) ValidateRequest(clusterID, token string) error {
	if clusterID != a.cfg.ClusterID {
		return ErrWrongClusterID
	}
	if token == a.cfg.PrimaryToken {
		return nil
	}
	if a.cfg.SecondaryToken != "" && token == a.cfg.SecondaryToken {
		return nil
	}
	return ErrInvalidToken
}
