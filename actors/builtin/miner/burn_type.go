package miner

type BurnType string

const (
	BurnTypeDisputeWindowedPoSt      BurnType = "DisputeWindowedPoSt"
	BurnTypePreCommitSectorBatch     BurnType = "PreCommitSectorBatch"
	BurnTypeProveCommitAggregate     BurnType = "ProveCommitAggregate"
	BurnTypeDeclareFaultsRecovered   BurnType = "DeclareFaultsRecovered"
	BurnTypeApplyRewards             BurnType = "ApplyRewards"
	BurnTypeReportConsensusFault     BurnType = "ReportConsensusFault"
	BurnTypeWithdrawBalance          BurnType = "WithdrawBalance "
	BurnTypeRepayDebt                BurnType = "RepayDebt"
	BurnTypeProcessEarlyTerminations BurnType = "processEarlyTerminations"
	BurnTypeHandleProvingDeadline    BurnType = "handleProvingDeadline "
)
