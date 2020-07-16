package adt

// MutationPermission is the mutation permission on a state field
type MutationPermission int

const (
	// Invalid means NO permission
	InvalidPermission MutationPermission = iota
	// ReadOnlyPermission allows reading but not mutating the field
	ReadOnlyPermission
	// WritePermission allows mutating the field
	WritePermission
)
