package reward

import (
	"testing"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
)

var mintingTestVectors = []struct {
	in  abi.ChainEpoch
	out string
}{
	{1051897, "135060784589637453410950129"},
	{2103794, "255386271058940593613485187"},
	{3155691, "362584098600550296025821387"},
	{4207588, "458086510989070493849325308"},
	{5259485, "543169492437427724953202180"},
	{6311382, "618969815707708523300124489"},
	{7363279, "686500230252085183344830372"},
}

func TestMintingFunction(t *testing.T) {
	for _, vector := range mintingTestVectors {
		ts_output := big.Rsh(taylorSeriesExpansion(vector.in), FixedPoint-90)
		expected_output, _ := big.FromString(vector.out)
		if !(ts_output.Equals(expected_output)) {
			t.Errorf("at epoch %q, computed supply %q, expected supply %q", vector.in, ts_output, expected_output)
		}
	}
}
