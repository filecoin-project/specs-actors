package models

type Actor struct {
	ParentStateRoot string
	Head            string
	Code            string
	Balance         string
	Nonce           uint64 `pg:",use_zero"`
}
