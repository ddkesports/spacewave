//go:build !js

package spacewave_cli

import (
	"os"
	"strconv"
	"time"

	"github.com/aperturerobotics/cli"
	"github.com/pkg/errors"
	block_gc "github.com/s4wave/spacewave/db/block/gc"
	"github.com/sirupsen/logrus"
)

// debugVolumeOwnersArgs are the arguments of the offline owner attribution.
type debugVolumeOwnersArgs struct {
	// kinds lists the owner kinds to cut, one walk each.
	kinds cli.StringSlice
}

// BuildFlags returns the owner attribution flags.
func (a *debugVolumeOwnersArgs) BuildFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringSliceFlag{
			Name:        "kind",
			Usage:       "owner kind to cut, such as bucket or head, repeatable",
			Value:       cli.NewStringSlice("bucket", "head", "provider", "reader"),
			Destination: &a.kinds,
		},
	}
}

// Run walks the stopped volume's ref graph once whole and once without each
// kind, printing each result as its walk finishes.
func (a *debugVolumeOwnersArgs) Run(c *cli.Context) error {
	// Open the stopped volume at the argument's path.
	if c.NArg() != 1 {
		return errors.New("expected one volume file argument")
	}
	path := c.Args().First()
	vol, err := openStoppedVolume(c.Context, logrus.NewEntry(logrus.New()), path)
	if err != nil {
		return err
	}
	defer vol.Close()
	rg, ok := vol.GetRefGraph().(*block_gc.RefGraph)
	if !ok {
		return errors.New("volume has no local ref graph")
	}

	// Mark the whole graph for the baseline.
	start := time.Now()
	all, err := rg.Reachable(c.Context)
	if err != nil {
		return errors.Wrap(err, "mark the whole graph")
	}
	live := all.Live.Len()
	writeFields(os.Stdout, [][2]string{
		{"Volume", path},
		{"Live nodes", strconv.Itoa(live)},
		{"Unknown blocks", strconv.Itoa(all.Unknown)},
		{"Walk", time.Since(start).Round(time.Second).String()},
	})

	// Mark again without each kind; the difference is what only it keeps.
	for _, kind := range a.kinds.Value() {
		start = time.Now()
		without, err := rg.ReachableWithout(c.Context, kind)
		if err != nil {
			return errors.Wrapf(err, "mark without %s", kind)
		}
		kept := live - without.Live.Len()
		writeFields(os.Stdout, [][2]string{
			{"Cut", kind},
			{"Live without", strconv.Itoa(without.Live.Len())},
			{"Kept only by " + kind, strconv.Itoa(kept) + " (" + strconv.FormatFloat(100*float64(kept)/float64(live), 'f', 1, 64) + "%)"},
			{"Walk", time.Since(start).Round(time.Second).String()},
		})
	}
	return nil
}

// newDebugVolumeOwnersCommand builds the offline owner attribution.
func newDebugVolumeOwnersCommand() *cli.Command {
	args := &debugVolumeOwnersArgs{}
	return &cli.Command{
		Name:      "volume-owners",
		Usage:     "count the ref graph nodes only each owner kind keeps live",
		ArgsUsage: "<volume-file>",
		Description: "Marks a stopped volume's ref graph from its roots once whole, then once for each kind " +
			"with that kind's owners reached but not walked, and prints how many nodes each walk lost: " +
			"the nodes only owners of that kind keep. Each walk reads the whole graph, so on a large " +
			"volume each takes about as long as a block pass's mark.",
		Flags:  args.BuildFlags(),
		Action: args.Run,
	}
}
