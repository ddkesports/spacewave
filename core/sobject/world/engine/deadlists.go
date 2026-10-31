package sobject_world_engine

import (
	"slices"
	"sync"

	"github.com/s4wave/spacewave/db/block"
)

// maxPendingDeadlists bounds the pending records. Past it the oldest is
// orphaned and leaks.
const maxPendingDeadlists = 64

// deadlistState is where a deadlist record is in its life.
type deadlistState uint8

const (
	// deadlistPending waits under block.DeadlistRootName for the base to adopt
	// a World on its chain and for the reader pins before that adoption.
	deadlistPending deadlistState = iota
	// deadlistDue moves to block.DeadlistDueRootName, where the collector
	// handles and releases it.
	deadlistDue
	// deadlistOrphan is off the accepted chain: its name is released and its
	// deaths leak.
	deadlistOrphan
)

// deadlistEntry is one named World's deadlist record in the ledger.
type deadlistEntry struct {
	// seq is the record's sequence.
	seq uint64
	// from is the root of the named World the interval starts at, or nil when
	// the interval before it forfeited.
	from *block.BlockRef
	// to is the root of the named World the interval ends at.
	to *block.BlockRef
	// record is the record's first block, or nil when the interval forfeited
	// and nothing is named.
	record *block.BlockRef
	// puts are the record's blocks until a durable save stores them.
	puts []*block.PutBatchEntry
	// state is where the record is.
	state deadlistState
	// stored is set once a durable save named the record as pending, so its
	// blocks are in the store.
	stored bool
	// carried is the save generation that carries state, or 0 when no save
	// does yet.
	carried uint64
}

// deadlistLedger tracks an engine's deadlist records from naming to due. A
// record is pending until the base adopts a World on its chain and every
// reader pin open before that adoption is released, then due: its name moves
// from block.DeadlistRootName to block.DeadlistDueRootName for the collector.
// A record the base can no longer reach through a chain of composed records is
// orphaned: its name is released and its deaths leak.
//
// Each save carries the changes no save carries yet, under the next
// generation. A durable generation confirms its changes; a failed one returns
// them to be carried again. Every name a save sets carries the record's blocks
// unless a durable save stored them, so no save names a missing block.
type deadlistLedger struct {
	// entries are the records in sequence order: pending ones, and due and
	// orphaned ones until a durable save carries them.
	entries []deadlistEntry
	// next is the sequence of the next record.
	next uint64
	// adopted is the sequence of the latest record whose World the base
	// adopted, or 0.
	adopted uint64
	// gen is the last save generation.
	gen uint64
}

// name adds the record of what died from the named World at from to the one
// at to. ok is false when the interval forfeited; the entry then names no
// record but keeps its place in the chain.
func (l *deadlistLedger) name(from, to *block.BlockRef, dead *block.Replacements, ok bool) error {
	// Encode the record.
	entry := deadlistEntry{seq: max(l.next, 1), from: from, to: to}
	if ok {
		puts, record, err := block.EncodeDeadlist(entry.seq, from, to, dead)
		if err != nil {
			return err
		}
		entry.record, entry.puts = record, puts
	}
	l.next = entry.seq + 1

	// Orphan the oldest pending record when the ledger is full.
	pending := l.pending()
	if len(pending) >= maxPendingDeadlists {
		l.move(pending[0], deadlistOrphan)
	}
	l.entries = append(l.entries, entry)
	return nil
}

// pending returns the indexes of the pending entries.
func (l *deadlistLedger) pending() []int {
	var out []int
	for i, e := range l.entries {
		if e.state == deadlistPending {
			out = append(out, i)
		}
	}
	return out
}

// move moves the entry at i to state, to be carried by the next save. An
// orphan nothing names and no save is naming is done at once.
func (l *deadlistLedger) move(i int, state deadlistState) {
	e := &l.entries[i]
	if state == deadlistOrphan && !e.stored && e.carried == 0 {
		e.record = nil
	}
	e.state, e.carried = state, 0
}

// compact drops the due and orphaned entries with nothing left to carry.
func (l *deadlistLedger) compact() {
	l.entries = slices.DeleteFunc(l.entries, func(e deadlistEntry) bool {
		return e.state != deadlistPending && e.record == nil
	})
}

// adopt records that the base adopted the World at root. When a pending record
// ends at root, it and the pending records before it joined to it by a chain
// of composed records are on the accepted chain; the records after the last
// adoption and before that chain are orphaned.
func (l *deadlistLedger) adopt(root *block.BlockRef) {
	// Find the latest pending record ending at root.
	pending := l.pending()
	j := len(pending) - 1
	for j >= 0 && !l.entries[pending[j]].to.EqualsRef(root) {
		j--
	}
	if j < 0 {
		return
	}

	// Walk back while each record composed from the one before it.
	start := j
	for start > 0 {
		next, prev := l.entries[pending[start]], l.entries[pending[start-1]]
		if next.record == nil || next.from == nil || !next.from.EqualsRef(prev.to) {
			break
		}
		start--
	}

	// Orphan the records before the chain that no earlier adoption accepted.
	for _, i := range pending[:start] {
		if l.entries[i].seq > l.adopted {
			l.move(i, deadlistOrphan)
		}
	}
	l.adopted = max(l.adopted, l.entries[pending[j]].seq)
	l.compact()
}

// advance moves the pending records at or below the adopted record, and below
// the oldest open reader pin stamp when one is lower, to due. It reports
// whether any moved.
func (l *deadlistLedger) advance(oldestPin uint64, pinned bool) bool {
	// Hold the watermark below the oldest pin.
	watermark := l.adopted
	if pinned {
		watermark = min(watermark, oldestPin)
	}

	// Move the pending records it passes.
	moved := false
	for _, i := range l.pending() {
		if l.entries[i].seq > watermark {
			break
		}
		l.move(i, deadlistDue)
		moved = true
	}
	l.compact()
	return moved
}

// carry returns what the next save stores: the blocks and names of each change
// no save carries yet. saved marks them carried.
func (l *deadlistLedger) carry() ([]*block.PutBatchEntry, []block.NamedRoot) {
	var puts []*block.PutBatchEntry
	var roots []block.NamedRoot
	for _, e := range l.entries {
		if e.carried != 0 || e.record == nil {
			continue
		}
		if !e.stored && e.state != deadlistOrphan {
			puts = append(puts, e.puts...)
		}
		pendingName := block.DeadlistRootName(e.seq)
		switch e.state {
		case deadlistPending:
			roots = append(roots, block.NamedRoot{Name: pendingName, Ref: e.record})
		case deadlistDue:
			roots = append(roots,
				block.NamedRoot{Name: block.DeadlistDueRootName(e.seq), Ref: e.record},
				block.NamedRoot{Name: pendingName},
			)
		case deadlistOrphan:
			roots = append(roots, block.NamedRoot{Name: pendingName})
		}
	}
	return puts, roots
}

// saved marks what carry returned as carried by a new save generation, which
// it returns.
func (l *deadlistLedger) saved() uint64 {
	l.gen++
	for i := range l.entries {
		if l.entries[i].carried == 0 {
			l.entries[i].carried = l.gen
		}
	}
	return l.gen
}

// confirm records that the saves through gen are durable. A pending record
// they carried is stored; a due or orphaned one is done.
func (l *deadlistLedger) confirm(gen uint64) {
	for i := range l.entries {
		e := &l.entries[i]
		if e.carried == 0 || e.carried > gen {
			continue
		}
		if e.state == deadlistPending {
			e.stored, e.puts = true, nil
			continue
		}
		e.record = nil
	}
	l.compact()
}

// unsave records that no save after the last confirmed one is durable, so the
// next save carries their changes again.
func (l *deadlistLedger) unsave() {
	for i := range l.entries {
		l.entries[i].carried = 0
	}
}

// encode returns the pending records as a cursor carried with the next save
// records them. Each is named once that save is durable: it carries every
// change no durable save carried.
func (l *deadlistLedger) encode() []*ReplayDeadlist {
	var out []*ReplayDeadlist
	for _, e := range l.entries {
		if e.state == deadlistPending {
			out = append(out, &ReplayDeadlist{Seq: e.seq, From: e.from, To: e.to, Record: e.record})
		}
	}
	return out
}

// restoreDeadlists returns the ledger a saved cursor recorded.
func restoreDeadlists(cursor *ReplayCursor) deadlistLedger {
	l := deadlistLedger{next: cursor.GetNextDeadlist(), adopted: cursor.GetAdoptedDeadlist()}
	for _, d := range cursor.GetDeadlists() {
		e := deadlistEntry{seq: d.GetSeq(), from: d.GetFrom(), to: d.GetTo(), record: d.GetRecord()}
		e.stored = e.record != nil
		l.entries = append(l.entries, e)
	}
	return l
}

// readerStamps stamps each reader pin with the adopted record sequence at the
// time it was taken, so a record waits for the readers that may hold a World
// before it. It is safe for concurrent use.
type readerStamps struct {
	// mtx guards the fields below.
	mtx sync.Mutex
	// epoch is the adopted record sequence new pins take.
	epoch uint64
	// open counts the open pins by stamp.
	open map[uint64]int
}

// setEpoch sets the stamp new pins take.
func (s *readerStamps) setEpoch(epoch uint64) {
	s.mtx.Lock()
	s.epoch = epoch
	s.mtx.Unlock()
}

// stamp records a pin and returns its release, which also releases the pin.
func (s *readerStamps) stamp(release func()) func() {
	// Count the pin under the current epoch.
	s.mtx.Lock()
	epoch := s.epoch
	if s.open == nil {
		s.open = make(map[uint64]int)
	}
	s.open[epoch]++
	s.mtx.Unlock()

	// Drop it once on release.
	return sync.OnceFunc(func() {
		release()
		s.mtx.Lock()
		if s.open[epoch]--; s.open[epoch] == 0 {
			delete(s.open, epoch)
		}
		s.mtx.Unlock()
	})
}

// oldest returns the lowest stamp of an open pin. pinned is false when none is
// open.
func (s *readerStamps) oldest() (stamp uint64, pinned bool) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	for epoch := range s.open {
		if !pinned || epoch < stamp {
			stamp, pinned = epoch, true
		}
	}
	return stamp, pinned
}
