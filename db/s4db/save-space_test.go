package s4db

import (
	"context"
	"math/rand/v2"
	"path/filepath"
	"testing"
)

// TestCheckpointSavesSpaceCopy commits while a background checkpoint
// encodes its copy of the space without the writer lock, so under the race
// detector the copy's trees change beside the space's, then checks the
// space and the contents before and after a reopen. The saved space is the
// copy, as of the last commit before the lock was released. With outgrow,
// the copy gains free runs that lengthen its encoding past the record's
// pages, so the checkpoint drops it and saves the space as of the commits
// made meanwhile.
func TestCheckpointSavesSpaceCopy(t *testing.T) {
	for _, outgrow := range []bool{false, true} {
		name := "fits"
		if outgrow {
			name = "outgrows"
		}
		t.Run(name, func(t *testing.T) {
			// Fill a file with random commits.
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "save.s4wave")
			db := openTest(t, path, Options{})
			r := rand.New(rand.NewPCG(1, 2)) // #nosec G404 -- test data.
			m := model{}
			for range 40 {
				randomCommit(t, r, db, m, 2000)
			}

			// Commit between the checkpoint's writer lock holds, then keep
			// committing while it encodes. With outgrow add runs to the copy
			// past the file end.
			var before, during uint64
			var hookErr, bgErr error
			bg := make(chan struct{})
			db.w.beforeSave = func(tr *tree) {
				// Note the seqno, grow the copy, and commit while unlocked.
				before = db.cur.Load().seq
				if sp := tr.save.sp; outgrow {
					for i := range uint64(4096) {
						sp.free.Set(sp.end+1<<20+2*i, 1)
					}
				}
				for range 3 {
					if _, err := commitRandom(r, db, m, 2000); err != nil {
						hookErr = err
						return
					}
				}

				// Keep committing in the background while the copy encodes.
				during = db.cur.Load().seq
				go func() {
					defer close(bg)
					for range 5 {
						if _, err := commitRandom(r, db, m, 2000); err != nil {
							bgErr = err
							return
						}
					}
				}()
			}

			// Run a checkpoint and wait for it.
			if err := db.w.lock(ctx); err != nil {
				t.Fatal(err)
			}
			db.w.startCheckpoint()
			db.w.unlock(nil)
			db.w.builds.Wait()
			if hookErr != nil {
				t.Fatal(hookErr)
			}
			<-bg
			if bgErr != nil {
				t.Fatal(bgErr)
			}
			if err := db.w.ckptErr; err != nil {
				t.Fatal(err)
			}

			// The superblock names the copy, or with outgrow the space as
			// of at least the hook's commits.
			sb := db.cur.Load().sb
			if sb.gen < 2 || (!outgrow && sb.spaceSeq != before) || (outgrow && sb.spaceSeq < during) {
				t.Fatalf("superblock gen %d saves the space at %d (lock released at %d, hook committed through %d)", sb.gen, sb.spaceSeq, before, during)
			}
			checkSpace(t, db)
			m.check(t, db)

			// The reopened file replays the commits after the saved space.
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = openTest(t, path, Options{})
			defer db.Close()
			checkSpace(t, db)
			m.check(t, db)
		})
	}
}

// TestCheckpointReplacedDuringEncode runs a checkpoint under the writer lock
// while a background checkpoint encodes its copy of the space, so under the
// race detector the two copies must not share their live counts. The
// background tree is dropped for the checkpoint that replaced it, and the
// space and contents survive a reopen.
func TestCheckpointReplacedDuringEncode(t *testing.T) {
	// Fill a file with random commits.
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "replace.s4wave")
	db := openTest(t, path, Options{})
	r := rand.New(rand.NewPCG(3, 4)) // #nosec G404 -- test data.
	m := model{}
	for range 40 {
		randomCommit(t, r, db, m, 2000)
	}

	// Commit, then take the lock before the background checkpoint encodes,
	// so the checkpoint under it copies the space while the encode reads its
	// own copy.
	var start uint64
	var fgErr error
	fg, held := make(chan struct{}), make(chan struct{})
	db.w.beforeSave = func(*tree) {
		// Note the generation and return once the other checkpoint holds
		// the lock.
		start = db.cur.Load().sb.gen
		go func() {
			// Commit, take the lock, and checkpoint under it.
			defer close(fg)
			defer close(held)
			if _, fgErr = commitRandom(r, db, m, 2000); fgErr != nil {
				return
			}
			if fgErr = db.w.lock(ctx); fgErr != nil {
				return
			}
			held <- struct{}{}
			fgErr = db.w.checkpoint(ctx)
			db.w.unlock(nil)
		}()
		<-held
	}

	// Run the background checkpoint and wait for both.
	if err := db.w.lock(ctx); err != nil {
		t.Fatal(err)
	}
	db.w.startCheckpoint()
	db.w.unlock(nil)
	db.w.builds.Wait()
	<-fg
	if fgErr != nil {
		t.Fatal(fgErr)
	}
	if err := db.w.ckptErr; err != nil {
		t.Fatal(err)
	}

	// A checkpoint saved after the encode began, and the space adds up.
	if sb := db.cur.Load().sb; sb.gen <= start {
		t.Fatalf("superblock gen %d, background encode began at gen %d", sb.gen, start)
	}
	checkSpace(t, db)
	m.check(t, db)

	// The reopened file holds the same space and contents.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openTest(t, path, Options{})
	defer db.Close()
	checkSpace(t, db)
	m.check(t, db)
}
