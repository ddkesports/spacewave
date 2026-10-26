package storebench

import (
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"time"
)

// maxStallDumps bounds the goroutine dumps one load writes.
const maxStallDumps = 32

// watchStall starts a timer that dumps every goroutine's stack when the write
// still runs after the configured stall threshold. The caller stops the
// returned timer when the write ends. It returns nil when dumps are off.
func (l *load) watchStall(seq uint64) *time.Timer {
	if l.conf.StallDumpDir == "" || l.conf.StallThreshold <= 0 {
		return nil
	}
	return time.AfterFunc(l.conf.StallThreshold, func() { l.dumpStall(seq) })
}

// dumpStall writes every goroutine's stack while write seq is stalled.
func (l *load) dumpStall(seq uint64) {
	// Skip dumps past the bound and outside the measured window.
	if !l.measuring.Load() {
		return
	}
	n := l.stallDumps.Add(1)
	if n > maxStallDumps {
		return
	}

	// Write the goroutine stacks to a file named by the dump and the write.
	name := "stall-" + strconv.FormatUint(n, 10) + "-write-" + strconv.FormatUint(seq, 10) + ".txt"
	f, err := os.Create(filepath.Join(l.conf.StallDumpDir, name))
	if err != nil {
		l.le.WithError(err).Warn("create stall dump")
		return
	}
	defer f.Close()
	if err := pprof.Lookup("goroutine").WriteTo(f, 2); err != nil {
		l.le.WithError(err).Warn("write stall dump")
	}
	l.le.WithField("dump", name).Info("write stalled; dumped goroutines")
}
