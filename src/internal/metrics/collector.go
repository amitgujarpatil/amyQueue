package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/yourusername/amyqueue/src/internal/raft"
)

// Collector implements prometheus.Collector. It reads a MetricsSnapshot from
// the raft node on every Prometheus scrape and translates it into metric
// values. The raft package never imports prometheus — this is the only bridge.
type Collector struct {
	src raft.MetricsSource

	// gauges — current state
	currentTerm   *prometheus.Desc
	isLeader      *prometheus.Desc
	commitIndex   *prometheus.Desc
	lastApplied   *prometheus.Desc
	memberCount   *prometheus.Desc
	voterCount    *prometheus.Desc
	observerCount *prometheus.Desc
	observerLag   *prometheus.Desc

	// counters — cumulative, only increase
	electionsTotal        *prometheus.Desc
	leaderChangesTotal    *prometheus.Desc
	heartbeatFailuresTotal *prometheus.Desc
}

func NewCollector(src raft.MetricsSource) *Collector {
	const ns = "amyqueue_raft"
	labels := []string{"node_id"}

	return &Collector{
		src: src,

		currentTerm: prometheus.NewDesc(
			ns+"_current_term",
			"Current Raft term.",
			labels, nil),
		isLeader: prometheus.NewDesc(
			ns+"_is_leader",
			"1 if this node is the current leader, 0 otherwise.",
			labels, nil),
		commitIndex: prometheus.NewDesc(
			ns+"_commit_index",
			"Highest log index committed by a quorum.",
			labels, nil),
		lastApplied: prometheus.NewDesc(
			ns+"_last_applied",
			"Highest log index applied to the state machine.",
			labels, nil),
		memberCount: prometheus.NewDesc(
			ns+"_member_count",
			"Total cluster members (voters + observers).",
			labels, nil),
		voterCount: prometheus.NewDesc(
			ns+"_voter_count",
			"Number of voting members.",
			labels, nil),
		observerCount: prometheus.NewDesc(
			ns+"_observer_count",
			"Number of observer (non-voting) members.",
			labels, nil),
		observerLag: prometheus.NewDesc(
			ns+"_observer_lag",
			"Log entries the observer is behind the leader (leader only).",
			[]string{"node_id", "observer_id"}, nil),

		electionsTotal: prometheus.NewDesc(
			ns+"_elections_total",
			"Total elections started by this node. A rising value indicates instability.",
			labels, nil),
		leaderChangesTotal: prometheus.NewDesc(
			ns+"_leader_changes_total",
			"Total times this node observed a new leader.",
			labels, nil),
		heartbeatFailuresTotal: prometheus.NewDesc(
			ns+"_heartbeat_failures_total",
			"Total failed AppendEntries RPCs per peer.",
			[]string{"node_id", "peer"}, nil),
	}
}

// Describe sends all metric descriptors to Prometheus. Called once at
// registration time.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.currentTerm
	ch <- c.isLeader
	ch <- c.commitIndex
	ch <- c.lastApplied
	ch <- c.memberCount
	ch <- c.voterCount
	ch <- c.observerCount
	ch <- c.observerLag
	ch <- c.electionsTotal
	ch <- c.leaderChangesTotal
	ch <- c.heartbeatFailuresTotal
}

// Collect is called by Prometheus on every scrape. It reads a fresh snapshot
// from the node and emits all metric values.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	s := c.src.MetricsSnapshot()
	id := s.NodeID

	var leaderVal float64
	if s.State == raft.Leader {
		leaderVal = 1
	}

	voters, observers := 0, 0
	for _, m := range s.Members {
		if m.State == raft.NodeVoter {
			voters++
		} else {
			observers++
		}
	}

	ch <- prometheus.MustNewConstMetric(c.currentTerm, prometheus.GaugeValue, float64(s.Term), id)
	ch <- prometheus.MustNewConstMetric(c.isLeader, prometheus.GaugeValue, leaderVal, id)
	ch <- prometheus.MustNewConstMetric(c.commitIndex, prometheus.GaugeValue, float64(s.CommitIndex), id)
	ch <- prometheus.MustNewConstMetric(c.lastApplied, prometheus.GaugeValue, float64(s.LastApplied), id)
	ch <- prometheus.MustNewConstMetric(c.memberCount, prometheus.GaugeValue, float64(len(s.Members)), id)
	ch <- prometheus.MustNewConstMetric(c.voterCount, prometheus.GaugeValue, float64(voters), id)
	ch <- prometheus.MustNewConstMetric(c.observerCount, prometheus.GaugeValue, float64(observers), id)

	for observerID, lag := range s.ObserverLag {
		ch <- prometheus.MustNewConstMetric(c.observerLag, prometheus.GaugeValue, float64(lag), id, observerID)
	}

	ch <- prometheus.MustNewConstMetric(c.electionsTotal, prometheus.CounterValue, float64(s.ElectionsStarted), id)
	ch <- prometheus.MustNewConstMetric(c.leaderChangesTotal, prometheus.CounterValue, float64(s.LeaderChanges), id)

	for peer, count := range s.HeartbeatFailures {
		ch <- prometheus.MustNewConstMetric(c.heartbeatFailuresTotal, prometheus.CounterValue, float64(count), id, peer)
	}
}
