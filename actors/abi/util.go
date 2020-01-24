package abi

type CronEventType int64

const (
	CronEventType_Miner_StateCleanup       = CronEventType(1)
	CronEventType_Miner_SurpriseExpiration = CronEventType(2)
	CronEventType_Miner_WorkerKeyChange    = CronEventType(3)
)

func assertNoError(e error) {
	if e != nil {
		panic(e.Error())
	}
}
