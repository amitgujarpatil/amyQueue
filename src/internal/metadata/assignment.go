package metadata

// AssignReplicas distributes partitions across brokers using a globally aware
// greedy algorithm that minimises blast radius on single-broker failure.
//
// Scoring per broker (lower = preferred):
//
//	score = (global_leader_count × α) + (leaders_from_this_topic × β)
//	α = 1            — load balance weight
//	β = len(brokers) — failure domain isolation weight (β >> α)
//
// β >> α guarantees that failure domain isolation wins over pure load balance:
// each topic's leaders land on different brokers before any broker gets a second
// leader from this topic.
//
// The function is pure: same inputs always produce the same output.
// It does not mutate leaderCounts — callers pass the live global counts
// read from the store immediately before calling.
//
// Returns nil when brokers is empty, numPartitions <= 0, or replicationFactor <= 0.
// Caps actual RF at len(brokers) — under-replication is detectable by the caller.
func AssignReplicas(
	brokers []BrokerID,
	numPartitions int32,
	replicationFactor int32,
	leaderCounts map[BrokerID]int,
) [][]BrokerID {
	if len(brokers) == 0 || numPartitions <= 0 || replicationFactor <= 0 {
		return nil
	}

	alpha := 1
	beta := len(brokers)

	// Work on a local copy of global leader counts so this function stays pure.
	globalCounts := make(map[BrokerID]int, len(leaderCounts))
	for b, c := range leaderCounts {
		globalCounts[b] = c
	}

	// Tracks leaders assigned to THIS topic during this call (used in β term).
	topicLeaders := make(map[BrokerID]int)

	effectiveRF := int(replicationFactor)
	if effectiveRF > len(brokers) {
		effectiveRF = len(brokers)
	}

	result := make([][]BrokerID, numPartitions)

	for p := int32(0); p < numPartitions; p++ {
		assigned := make(map[BrokerID]bool, effectiveRF)
		replicas := make([]BrokerID, 0, effectiveRF)

		for slot := 0; slot < effectiveRF; slot++ {
			best := pickBest(brokers, assigned, globalCounts, topicLeaders, alpha, beta)
			replicas = append(replicas, best)
			assigned[best] = true

			// Only the first slot (the leader) counts toward scoring.
			if slot == 0 {
				topicLeaders[best]++
				globalCounts[best]++
			}
		}

		result[p] = replicas
	}

	return result
}

// pickBest selects the broker with the lowest score that hasn't been assigned
// to this partition yet.
func pickBest(
	brokers []BrokerID,
	assigned map[BrokerID]bool,
	globalCounts map[BrokerID]int,
	topicLeaders map[BrokerID]int,
	alpha, beta int,
) BrokerID {
	var best BrokerID
	bestScore := int(^uint(0) >> 1) // MaxInt

	for _, b := range brokers {
		if assigned[b] {
			continue
		}
		score := globalCounts[b]*alpha + topicLeaders[b]*beta
		if score < bestScore {
			bestScore = score
			best = b
		}
	}
	return best
}
