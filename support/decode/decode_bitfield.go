package main

import (
	"encoding/hex"
	"fmt"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/big"
	"os"
	"sort"

	"github.com/urfave/cli/v2"
)

var bfDecodeCmd = &cli.Command{
	Name:        "bf",
	Description: "decode bitfield from base64 bytes",
	Action:      runDecodeBFCmd,
}

var intDecodeCmd = &cli.Command{
	Name:        "int",
	Description: "decode big.Int from base64 bytes",
	Action:      runDecodeIntCmd,
}

func main() {
	app := &cli.App{
		Name:        "decode",
		Usage:       "Decode a hex encoded data structure",
		Description: "Decode a hex encoded data structure",
		Commands: []*cli.Command{
			bfDecodeCmd,
			intDecodeCmd,
		},
	}
	sort.Sort(cli.CommandsByName(app.Commands))
	for _, c := range app.Commands {
		sort.Sort(cli.FlagsByName(c.Flags))
	}
	err := app.Run(os.Args)
	if err != nil {
		panic(err)
	}
}

func runDecodeBFCmd(ctx *cli.Context) error {
	hexString := ctx.Args().First()

	b, err := hex.DecodeString(hexString)
	if err != nil {
		return err
	}

	bf, err := bitfield.NewFromBytes(b)
	if err != nil {
		return err
	}

	bf.ForEach(func(u uint64) error {
		fmt.Println(u)
		return nil
	})

	return nil
}

func runDecodeIntCmd(ctx *cli.Context) error {
	hexString := ctx.Args().First()

	b, err := hex.DecodeString(hexString)
	if err != nil {
		return err
	}

	i, err := big.FromBytes(b)
	if err != nil {
		return err
	}

	fmt.Println(i)

	return nil
}
