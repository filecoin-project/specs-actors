package util

import (
	"fmt"
	"github.com/filecoin-project/go-state-types/exitcode"
)

type abort struct {
	code exitcode.ExitCode
	msg  string
}

// Indicates a condition that should never happen. If encountered, execution will halt and the
// resulting state is undefined.
func AssertMsg(b bool, format string, a ...interface{}) {
	if !b {
		panic(abort{exitcode.ErrForbidden, fmt.Sprintf(format, a...)})
	}
}

func Assert(b bool) {
	AssertMsg(b, "assertion failed")
}

func AssertNoError(e error) {
	if e != nil {
		panic(abort{exitcode.ErrForbidden, e.Error()})
	}
}
