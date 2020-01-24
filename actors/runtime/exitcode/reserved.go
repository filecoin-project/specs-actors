package exitcode

import "strconv"

type ExitCode int64

func (x ExitCode) IsSuccess() bool {
	return x == Ok
}

func (x ExitCode) IsError() bool {
	return !x.IsSuccess()
}

// Implement error to trigger Go compiler checking of exit code return values.
func (x ExitCode) Error() string {
	return strconv.FormatInt(int64(x), 10)
}

const (
	Ok = ExitCode(0)
	// NOTE: none of the system error codes should ever be used by actors, so we could move their definition
	// out of this package and into the runtime spec.

	// Indicates failure to find an actor in the state tree.
	SysErrActorNotFound = ExitCode(1)

	// Indicates failure to find the code for an actor.
	SysErrActorCodeNotFound = ExitCode(2)

	// Indicates failure to find a method in an actor.
	SysErrInvalidMethod = ExitCode(3)

	// Indicates syntactically invalid parameters for a method.
	SysErrInvalidParameters = ExitCode(4)

	// Indicates a message sender has insufficient funds for a message's execution.
	SysErrInsufficientFunds = ExitCode(5)

	// Indicates a message invocation out of sequence.
	SysErrInvalidCallSeqNum = ExitCode(6)

	// Indicates message execution (including subcalls) used more gas than the specified limit.
	SysErrOutOfGas = ExitCode(7)

	// Indicates a message execution is forbidden for the caller.
	SysErrForbidden = ExitCode(8)

	// Indicates actor code performed a disallowed operation. Disallowed operations include:
	// - mutating state outside of a state acquisition block
	// - failing to invoke caller validation
	// - aborting with a reserved exit code (including success or a system error).
	SysErrorIllegalActor = ExitCode(9)

	// Indicates an invalid argument passed to a runtime method.
	SysErrorIllegalArgument = ExitCode(10)

	// Indicates  an object failed to de/serialize for storage.
	SysErrSerialization = ExitCode(11)

	// Reserved exit codes, do not use.
	SysErrorReserved1 = ExitCode(12)
	SysErrorReserved2 = ExitCode(13)
	SysErrorReserved3 = ExitCode(14)

	// Indicates something broken within the VM.
	SysErrInternal = ExitCode(15)
)

// The initial range of exit codes is reserved for system errors.
// Actors may define codes starting with this one.
const FirstActorErrorCode = ExitCode(16)
