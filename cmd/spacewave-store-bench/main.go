//go:build !js

package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/aperturerobotics/cli"
)

func main() {
	// Register the run and seed commands.
	var runArgs RunArgs
	var seedArgs SeedArgs
	app := cli.NewApp()
	app.Name = "spacewave-store-bench"
	app.HideVersion = true
	app.Usage = "measure World write speed on a copy of a stopped server store"
	app.Commands = []*cli.Command{
		{
			Name:   "run",
			Usage:  "drive a synthetic server load on the store and print its report",
			Flags:  runArgs.BuildFlags(),
			Action: runArgs.Run,
		},
		{
			Name:   "seed",
			Usage:  "write a synthetic World into a new store",
			Flags:  seedArgs.BuildFlags(),
			Action: seedArgs.Run,
		},
	}

	// Run the command until it ends or the process is interrupted.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := app.RunContext(ctx, os.Args); err != nil {
		os.Stderr.WriteString(err.Error() + "\n")
		os.Exit(1)
	}
}
