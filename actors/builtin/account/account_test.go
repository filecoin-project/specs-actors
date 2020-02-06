package account_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/stretchr/testify/assert"

	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	account "github.com/filecoin-project/specs-actors/actors/builtin/account"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	mock "github.com/filecoin-project/specs-actors/support/mock"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
)

type constructorTestCase struct {
	desc     string
	addr     address.Address
	exitCode exitcode.ExitCode
}

func TestAccountactor(t *testing.T) {
	actor := account.AccountActor{}

	receiver := tutil.NewIDAddr(t, 100)
	builder := mock.NewBuilder(context.Background(), receiver).WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)

	testCases := []constructorTestCase{
		{
			desc:     "happy path construct SECP256K1 address",
			addr:     tutil.NewSECP256K1Addr(t, "secpaddress"),
			exitCode: exitcode.Ok,
		},
		{
			desc:     "happy path construct BLS address",
			addr:     tutil.NewBLSAddr(t, 1),
			exitCode: exitcode.Ok,
		},
		{
			desc:     "fail to construct account actor using ID address",
			addr:     tutil.NewIDAddr(t, 1),
			exitCode: exitcode.ErrIllegalArgument,
		},
		{
			desc:     "fail to construct account actor using Actor address",
			addr:     tutil.NewActorAddr(t, "actoraddress"),
			exitCode: exitcode.ErrIllegalArgument,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			rt := builder.Build(t)
			rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)

			if tc.exitCode.IsSuccess() {
				rt.Call(actor.Constructor, &tc.addr)

				var st account.AccountActorState
				rt.GetState(&st)
				assert.Equal(t, tc.addr, st.Address)

				pubkeyAddress := rt.Call(actor.PubkeyAddress, &adt.EmptyValue{}).(address.Address)
				assert.Equal(t, tc.addr, pubkeyAddress)
			} else {
				rt.ExpectAbort(tc.exitCode, func() {
					rt.Call(actor.Constructor, &tc.addr)
				})
			}
			rt.Verify()
		})
	}
}
