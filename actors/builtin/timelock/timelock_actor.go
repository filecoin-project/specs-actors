package timelock


type Actor struct{}

func (a Actor) Exports() []interface{} {
	return []interface{}{
		builtin.MethodConstructor: a.Constructor,
		2: a.LockTransaction,
		3: a.CronTick
	}
}


func (a Actor) Constructor(rt runtime.Runtime, _ abi.EmptyValue) {
	// sets up the state to have an empty map of times=>transaction sets
}



func (a Actor) LockTransaction(unlockEpoch abi.ChainEpoch, txEnc []byte){
	rt.CurrEpoch()

}

type TimelockTx struct {
	
}

// this will be executed at the end of every epoch
// (even null blocks!)
func (a Actor) CronTick(){
	rt.CurrEpoch()
	// loop over all tx registered for the current epoch
	// beacon := rt.BeaconEntryForDrandEpoch(FilecoinEpochToDrandEpoch(rt.CurrEpoch))
	// decrypt(beacon, st.DrandPublicKey, encTx)

}


// TODO state
type State struct {
	// map[abi.ChainEpoch]EncTxSet
	// DrandPublicKey
}
