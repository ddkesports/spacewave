//go:build !js

package main

import (
	"github.com/aperturerobotics/cli"
	"github.com/s4wave/spacewave/cmd/spacewave-store-bench/storebench"
)

// SeedArgs write a synthetic World into a new store.
type SeedArgs struct {
	StoreArgs
	// Seed shapes the World.
	Seed storebench.SeedConfig
}

// BuildFlags binds the store and World shape flags.
func (a *SeedArgs) BuildFlags() []cli.Flag {
	return append(a.StoreArgs.BuildFlags(),
		&cli.StringFlag{Name: "body-type", Usage: "body type of the created SharedObject", Destination: &a.Store.BodyType, Value: "store-bench"},
		&cli.IntFlag{Name: "objects", Usage: "objects to create", Destination: &a.Seed.Objects, Value: 100000},
		&cli.IntFlag{Name: "body-bytes", Usage: "body bytes of each object", Destination: &a.Seed.BodyBytes, Value: 3200},
		&cli.IntFlag{Name: "links", Usage: "graph links from each object to earlier ones", Destination: &a.Seed.Links, Value: 2},
		&cli.IntFlag{Name: "batch", Usage: "objects created per World write", Destination: &a.Seed.Batch, Value: 500},
		&cli.Uint64Flag{Name: "seed", Usage: "seeds the link targets", Destination: &a.Seed.Seed, Value: 1},
	)
}

// Run opens or creates the store's World and seeds it.
func (a *SeedArgs) Run(c *cli.Context) error {
	// Open the World, creating its SharedObject, then write the objects.
	ctx, le := c.Context, a.logger()
	st, err := storebench.OpenStore(ctx, le, &a.Store, true)
	if err != nil {
		return err
	}
	defer st.Close()
	return storebench.Seed(ctx, le, st.Engine, &a.Seed)
}
