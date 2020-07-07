package miner

// Identifier for a single partition within a miner.
type PartitionKey struct {
	Deadline uint64
	Partition uint64
}
