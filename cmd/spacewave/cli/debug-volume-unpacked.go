//go:build !js

package spacewave_cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aperturerobotics/cli"
	"github.com/s4wave/spacewave/db/s4db"
	store_kvkey "github.com/s4wave/spacewave/db/store/kvkey"
	"github.com/s4wave/spacewave/db/volume/packmove"
)

// unpackedArgs are the arguments of the volume-unpacked command.
type unpackedArgs struct {
	index packIndexArgs
	// indexPath is the scratch s4db file that holds the pack index.
	indexPath string
	// out is the key list to write.
	out string
}

// newDebugVolumeUnpackedCommand builds the command that lists the blocks of a
// volume file that no pack of a store holds.
func newDebugVolumeUnpackedCommand() *cli.Command {
	var args unpackedArgs
	return &cli.Command{
		Name:      "volume-unpacked",
		Usage:     "list the blocks of a volume file that no pack of the store holds",
		ArgsUsage: "<volume-file>",
		Description: "Indexes every pack of the store into the --index file, as volume-pack-index does, reading the end of " +
			"each pack once and resuming from what the file holds. Then opens the volume file read-only beside any process " +
			"that writes it and looks up each block key in that index, reading no pack. Writes the keys no pack holds, " +
			"of every store in the volume, to --out as one hex key per line in key order, ready for volume-pack --list. " +
			"The list appears under its name only once it is complete. A key that is not a block ref is listed too, " +
			"so the pack records it as bad.",
		Flags: append([]cli.Flag{
			&cli.StringFlag{Name: "index", Usage: "the scratch s4db file holding the pack index, created when missing", Destination: &args.indexPath, Required: true},
			&cli.StringFlag{Name: "out", Usage: "the key list to write", Destination: &args.out, Required: true},
			&cli.StringFlag{Name: "world-store", Usage: "the store id the index is keyed by", Value: "hyperline-world", Destination: &args.index.pack.worldStore},
			&cli.StringFlag{Name: "bucket", Usage: "the bucket that holds the packs", EnvVars: []string{"HYPERLINE_PROD_BUCKET"}, Destination: &args.index.pack.bucket},
			&cli.StringFlag{Name: "endpoint", Usage: "the S3 endpoint", Value: "https://s3.us-east-005.backblazeb2.com", EnvVars: []string{"HYPERLINE_PROD_BUCKET_ENDPOINT"}, Destination: &args.index.pack.endpoint},
			&cli.StringFlag{Name: "region", Usage: "the S3 region", Value: "us-east-005", EnvVars: []string{"HYPERLINE_PROD_BUCKET_REGION"}, Destination: &args.index.pack.region},
			&cli.StringFlag{Name: "key-prefix", Usage: "the key prefix of the packs", Value: "world/packs/", Destination: &args.index.pack.keyPrefix},
			&cli.IntFlag{Name: "width", Usage: "packs read at once; zero uses the server connections or 8", Destination: &args.index.width},
		}, ftpsFlags(&args.index.pack.ftps, "read the packs from")...),
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return errors.New("expected the volume file")
			}
			return listUnpacked(c.Context, c.Args().Get(0), &args, os.Stderr)
		},
	}
}

// listUnpacked indexes the store's packs and writes the blocks of the volume
// file at path that none holds to args.out, writing progress lines to w.
func listUnpacked(ctx context.Context, path string, args *unpackedArgs, w io.Writer) error {
	// Index every pack.
	idx, err := s4db.Open(args.indexPath, s4db.Options{})
	if err != nil {
		return err
	}
	defer idx.Close()
	packs, err := openIndexedPacks(idx, &args.index)
	if err != nil {
		return err
	}
	defer packs.Close()
	if err := indexPacks(ctx, packs, &args.index, w); err != nil {
		return err
	}

	// Open the volume beside its writer and the list beside its final name.
	vol, err := s4db.Open(path, s4db.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer vol.Close()
	partial := args.out + ".partial"
	out, err := os.Create(partial)
	if err != nil {
		return err
	}
	defer out.Close()

	// List the blocks no pack holds, then publish the list.
	start := time.Now()
	var last time.Time
	report := func(p packmove.UnpackedProgress) {
		if time.Since(last) < copyReportInterval {
			return
		}
		last = time.Now()
		fmt.Fprintf(w, "%s: %d blocks examined, %d unpacked\n", time.Since(start).Round(time.Second), p.Examined, p.Missing)
	}
	p, err := packmove.FindUnpacked(ctx, vol, store_kvkey.NewDefaultKVKey(), packs, out, report)
	if err != nil {
		return err
	}

	// Publish the list under its name once it is on disk.
	if err := out.Sync(); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	if err := os.Rename(partial, args.out); err != nil {
		return err
	}
	fmt.Fprintf(w, "%s: done: %d blocks examined, %d unpacked, %d of them not block refs; listed in %s\n",
		time.Since(start).Round(time.Second), p.Examined, p.Missing, p.Unparsed, args.out)
	return nil
}
