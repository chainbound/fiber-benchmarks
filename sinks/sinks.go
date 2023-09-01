package sinks

// InitType is used to specify which tables to create for the sink
type InitType string

const (
	Transactions InitType = "transactions"
	Blocks       InitType = "blocks"
)
