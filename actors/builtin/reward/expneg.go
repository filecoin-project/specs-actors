package reward

import (
	"math/big"
)

const precision = 128

var (
	// Coefficents in Q.128 format
	expNumCoef  []*big.Int
	expDenoCoef []*big.Int
)

func init() {
	parse := func(coefs []string) []*big.Int {
		out := make([]*big.Int, len(coefs))
		for i, coef := range coefs {
			c, ok := new(big.Int).SetString(coef, 10)
			if !ok {
				panic("could not parse exp paramemter")
			}
			// << 128 (Q.0 to Q.128) >> 128 to transform integer params to coefficients
			out[i] = c
		}
		return out
	}

	// parameters are in integer format,
	// coefficients are *2^-128 of that
	// so we can just load them if we treat them as Q.128
	num := []string{
		"-648770010757830093818553637600",
		"67469480939593786226847644286976",
		"-3197587544499098424029388939001856",
		"89244641121992890118377641805348864",
		"-1579656163641440567800982336819953664",
		"17685496037279256458459817590917169152",
		"-115682590513835356866803355398940131328",
		"340282366920938463463374607431768211456",
	}
	expNumCoef = parse(num)

	deno := []string{
		"1225524182432722209606361",
		"114095592300906098243859450",
		"5665570424063336070530214243",
		"194450132448609991765137938448",
		"5068267641632683791026134915072",
		"104716890604972796896895427629056",
		"1748338658439454459487681798864896",
		"23704654329841312470660182937960448",
		"259380097567996910282699886670381056",
		"2250336698853390384720606936038375424",
		"14978272436876548034486263159246028800",
		"72144088983913131323343765784380833792",
		"224599776407103106596571252037123047424",
		"340282366920938463463374607431768211456",
	}
	expDenoCoef = parse(deno)
}

// expneg accepts x in Q.128 format and computes e^-x.
// It is most precise within [0, 1.725) range, where error is less than 3.4e-30.
// Over the [0, 5) range its error is less than 4.6e-15.
// Output is in Q.128 format.
func expneg(x *big.Int) *big.Int {
	// exp is approximated by rational function
	// polynomials of the rational function are evaluated using Horner's method
	num := polyval(expNumCoef, x)   // Q.128
	deno := polyval(expDenoCoef, x) // Q.128

	num = num.Lsh(num, precision) // Q.256
	return num.Div(num, deno)     // Q.256 / Q.128 => Q.128
}

// polyval evaluates a polynomial given by coefficients `p` in Q.128 format
// at point `x` in Q.128 format. Output is in Q.128.
// Coefficients should be ordered from the highest order coefficient to the lowest.
func polyval(p []*big.Int, x *big.Int) *big.Int {
	// evaluation using Horner's method
	res := new(big.Int).Set(p[0]) // Q.128
	tmp := new(big.Int)           // big.Int.Mul doesn't like when input is reused as output
	for _, c := range p[1:] {
		tmp = tmp.Mul(res, x)         // Q.128 * Q.128 => Q.256
		res = res.Rsh(tmp, precision) // Q.256 >> 128 => Q.128
		res = res.Add(res, c)
	}

	return res
}
