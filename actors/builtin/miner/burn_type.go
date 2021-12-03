package miner

type BurnMethod string

const (
	BurnMethodDisputeWindowedPoSt      BurnMethod = "DisputeWindowedPoSt"
	BurnMethodPreCommitSectorBatch     BurnMethod = "PreCommitSectorBatch"
	BurnMethodProveCommitAggregate     BurnMethod = "ProveCommitAggregate"
	BurnMethodDeclareFaultsRecovered   BurnMethod = "DeclareFaultsRecovered"
	BurnMethodApplyRewards             BurnMethod = "ApplyRewards"
	BurnMethodReportConsensusFault     BurnMethod = "ReportConsensusFault"
	BurnMethodWithdrawBalance          BurnMethod = "WithdrawBalance "
	BurnMethodRepayDebt                BurnMethod = "RepayDebt"
	BurnMethodProcessEarlyTerminations BurnMethod = "ProcessEarlyTerminations"
	BurnMethodHandleProvingDeadline    BurnMethod = "HandleProvingDeadline "
)
