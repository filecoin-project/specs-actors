package builtin

import (
	"fmt"
)

// Accumulates a sequence of messages (e.g. validation failures).
type MessageAccumulator struct {
	msgs []string
}

func (ma *MessageAccumulator) IsEmpty() bool {
	return len(ma.msgs) == 0
}

func (ma *MessageAccumulator) Messages() []string {
	return ma.msgs[:]
}

// Adds messages to the accumulator.
func (ma *MessageAccumulator) Add(msgs ...string) {
	ma.msgs = append(ma.msgs, msgs...)
}

// Adds a message to the accumulator
func (ma *MessageAccumulator) Addf(msg string, args ...interface{}) {
	ma.Add(fmt.Sprintf(msg, args...))
}

// Adds messages from another accumulator to this one.
func (ma *MessageAccumulator) AddAll(msgs *MessageAccumulator) {
	ma.Add(msgs.msgs...)
}

// Adds a message if predicate is false.
func (ma *MessageAccumulator) Require(predicate bool, msg string, args ...interface{}) {
	if !predicate {
		ma.Add(fmt.Sprintf(msg, args...))
	}
}
