package vm

import (
	"crypto/sha256"
	"fmt"
	"os"
	"strings"

	"github.com/filecoin-project/go-address"
	abi "github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin"
)

//
// Test Vector generation utilities
//

type vectorGen struct {
	conformanceDir string
	determinismDir string
	vector         testVector
}

func newVectorGen() *vectorGen {
	// check environment variables to determine if generation is on
	conformanceDir := os.Getenv("SPECS_ACTORS_CONFORMANCE")
	determinismDir := os.Getenv("SPECS_ACTORS_DETERMINISM")
	return &vectorGen{
		conformanceDir: conformanceDir,
		determinismDir: determinismDir,
	}
}

func (g *vectorGen) determinism() bool {
	return g.determinismDir != ""
}

func (g *vectorGen) conformance() bool {
	return g.conformanceDir != ""
}

func (g *vectorGen) before(v *VM, name string) error {
	if g.determinism() || g.conformance() {
		// Set test vector pre application conditions
		startOpts := StartConditions(v, name)
		for _, opt := range startOpts {
			if err := opt(&(g.vector)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *vectorGen) after(v *VM, from, to address.Address, value abi.TokenAmount, method abi.MethodNum, params interface{}, callSeq uint64, result MessageResult, fakesAccessed bool, name string) error {
	if !g.conformance() && !g.determinism() {
		return nil
	}
	// Set test vector message and post application conditions
	if err := SetMessage(from, to, callSeq, value, method, params)(&(g.vector)); err != nil {
		return err
	}
	if err := SetEndStateTree(v.StateRoot(), v.store)(&(g.vector)); err != nil {
		return err
	}
	if err := SetReceipt(result)(&(g.vector)); err != nil {
		return err
	}
	vectorBytes, err := (&(g.vector)).MarshalJSON()
	if err != nil {
		return err
	}

	fromID, _ := v.NormalizeAddress(from)
	toID, _ := v.NormalizeAddress(to)
	act, _, _ := v.GetActor(toID)
	actName := strings.Split(builtin.ActorNameByCode(act.Code), "/")[2]

	h := sha256.Sum256(vectorBytes)
	fname := fmt.Sprintf("%x-%s-%s-%s-%d.json", string(h[:]), fromID, toID, actName, method)

	// Write conformance test-vectors
	if g.conformance() && !fakesAccessed {
		if err := writeVector(name, fname, vectorBytes, g.conformanceDir); err != nil {
			return err
		}
	}

	// Write determinism test-vectors
	if g.determinism() {
		if err := writeVector(name, fname, vectorBytes, g.determinismDir); err != nil {
			return err
		}
	}
	return nil
}

// rootDir is the top level directory containing all vectors
// dname is the subdirectory path containing the file to write
// fname is the name of this file
// vectorBytes is the data to write to file
func writeVector(dname, fname string, vectorBytes []byte, rootDir string) error {
	dir := rootDir + "/" + dname
	exists, err := dirExists(dir)
	if err != nil {
		return err
	}
	if !exists {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	return os.WriteFile(dir+"/"+fname, vectorBytes, 0755)
}

// dirExists returns whether the given file or directory exists
func dirExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
