package metadata

import "errors"

// ErrNoISRAlive is returned when all ISR members are dead and unclean election
// is disabled. The partition goes offline.
var ErrNoISRAlive = errors.New("all ISR members are dead and unclean election is disabled")

// ElectLeader selects a new partition leader from the ISR.
//
// Rules (matching KRaft semantics):
//  1. Prefer the first ISR member that is in aliveBrokers (alive priority).
//  2. If no ISR member is alive and uncleanAllowed=true, fall back to
//     any alive broker in the replica list (data loss risk — user opted in).
//  3. If no candidate found, return ErrNoISRAlive.
//
// The returned newISR is the subset of the original ISR whose members are alive.
// The leader is always the first element of newISR.
func ElectLeader(
	partition *PartitionState,
	aliveBrokers map[BrokerID]bool,
	uncleanAllowed bool,
) (newLeader BrokerID, newISR []BrokerID, err error) {
	// Build alive ISR subset.
	for _, id := range partition.ISR {
		if aliveBrokers[id] {
			newISR = append(newISR, id)
		}
	}

	if len(newISR) > 0 {
		return newISR[0], newISR, nil
	}

	// No alive ISR member.
	if !uncleanAllowed {
		return "", nil, ErrNoISRAlive
	}

	// Unclean election: accept any alive replica (may not have all data).
	for _, id := range partition.Replicas {
		if aliveBrokers[id] {
			return id, []BrokerID{id}, nil
		}
	}

	return "", nil, ErrNoISRAlive
}

// AliveBrokerSet converts a slice of BrokerIDs into a set for O(1) lookup.
func AliveBrokerSet(alive []BrokerID) map[BrokerID]bool {
	set := make(map[BrokerID]bool, len(alive))
	for _, id := range alive {
		set[id] = true
	}
	return set
}
