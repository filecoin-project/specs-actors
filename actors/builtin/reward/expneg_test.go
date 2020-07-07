package reward

import (
	"math/big"
	"testing"
)

var Res big.Word

func BenchmarkExpneg(b *testing.B) {
	x := new(big.Int).SetUint64(14)
	x = x.Lsh(x, precision-3) // set x to 1.75
	dec := new(big.Int)
	dec = dec.Div(x, big.NewInt(int64(b.N)))
	b.ResetTimer()
	b.ReportAllocs()
	var res big.Word

	for i := 0; i < b.N; i++ {
		r := expneg(x)
		res += r.Bits()[0]
		x.Sub(x, dec)
	}
	Res += res
}
