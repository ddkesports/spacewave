package sobject_world_engine

import (
	"sync"

	"github.com/s4wave/spacewave/db/block"
)

// maxWorldTransitions bounds the transitions worldDeaths holds between two
// compositions. An engine that never names a World, such as one installing
// another participant's checkpoints, reaches it and forfeits instead of
// growing.
const maxWorldTransitions = 4096

// worldTransition is one World commit: the root it made and what died.
type worldTransition struct {
	// to is the root the commit made.
	to *block.BlockRef
	// dead is what died, or nil when the commit forfeited its record.
	dead *block.Replacements
}

// worldDeaths gathers the World transitions the engine's block engines commit,
// local writes and replay alike, and composes the chain from the last named
// World to the next into what died between them.
//
// A local write publishes its World before replay accepts it, and replay may
// rebuild the World in another order, so a transition's record holds only if
// its World is on the accepted chain. Composition follows roots from the last
// named World, so a transition off the chain is never composed. A gap or a
// forfeited record on the chain forfeits the interval: what died in it leaks
// and is never reported. It is safe for concurrent use.
type worldDeaths struct {
	// mtx guards the fields below.
	mtx sync.Mutex
	// from is the root the next composition starts at, or nil when the
	// interval is already forfeited.
	from *block.BlockRef
	// next holds the transitions by the root they start from.
	next map[string][]worldTransition
	// count is the number of transitions in next.
	count int
}

// newWorldDeaths starts gathering at the World root from.
func newWorldDeaths(from *block.BlockRef) *worldDeaths {
	return &worldDeaths{from: from.Clone(), next: make(map[string][]worldTransition)}
}

// add records a commit's transition. It is the World block engines'
// TransitionSink.
func (d *worldDeaths) add(from, to *block.BlockRef, dead *block.Replacements) {
	// A commit that changed nothing has nothing to compose.
	if from.EqualsRef(to) {
		return
	}

	// Record it, or forfeit the interval when the table is full.
	d.mtx.Lock()
	defer d.mtx.Unlock()
	if d.count >= maxWorldTransitions {
		d.from, d.next, d.count = nil, make(map[string][]worldTransition), 0
		return
	}
	key := from.MarshalString()
	d.next[key] = append(d.next[key], worldTransition{to: to.Clone(), dead: dead})
	d.count++
}

// take composes what died from the last named World to the World at root, and
// starts the next interval at root. ok is false when no chain of recorded
// transitions reaches root or one on it forfeited; the interval then leaks.
// Only the transitions root reaches stay.
func (d *worldDeaths) take(root *block.BlockRef) (dead *block.Replacements, ok bool) {
	// Find a path from the last named World to root and compose it.
	d.mtx.Lock()
	defer d.mtx.Unlock()
	if d.from != nil {
		dead, ok = d.compose(d.from, root)
	}

	// Keep the transitions root reaches and start there.
	d.prune(root)
	d.from = root.Clone()
	return dead, ok
}

// compose finds a path of transitions from from to root, breadth first, and
// composes their records in order. Assumes mtx is locked.
func (d *worldDeaths) compose(from, root *block.BlockRef) (*block.Replacements, bool) {
	// An unchanged World lost nothing.
	if from.EqualsRef(root) {
		return &block.Replacements{}, true
	}

	// Walk the roots from from, remembering the transition into each.
	type step struct {
		prev string
		tr   worldTransition
	}
	start, goal := from.MarshalString(), root.MarshalString()
	into := map[string]step{start: {}}
	queue := []string{start}
	for len(queue) != 0 {
		key := queue[0]
		queue = queue[1:]
		if key == goal {
			break
		}
		for _, tr := range d.next[key] {
			toKey := tr.to.MarshalString()
			if _, seen := into[toKey]; !seen {
				into[toKey] = step{prev: key, tr: tr}
				queue = append(queue, toKey)
			}
		}
	}
	if _, found := into[goal]; !found {
		return nil, false
	}

	// Collect the path back from root, then compose it from the start.
	var path []*block.Replacements
	for key := goal; key != start; key = into[key].prev {
		if into[key].tr.dead == nil {
			return nil, false
		}
		path = append(path, into[key].tr.dead)
	}
	out := path[len(path)-1]
	for i := len(path) - 2; i >= 0; i-- {
		out = out.Then(path[i])
	}
	return out, true
}

// prune keeps the transitions root reaches. Assumes mtx is locked.
func (d *worldDeaths) prune(root *block.BlockRef) {
	// Walk the roots from root.
	kept := make(map[string][]worldTransition)
	count := 0
	queue := []string{root.MarshalString()}
	for len(queue) != 0 {
		key := queue[0]
		queue = queue[1:]
		if _, done := kept[key]; done {
			continue
		}
		trs := d.next[key]
		kept[key] = trs
		count += len(trs)
		for _, tr := range trs {
			queue = append(queue, tr.to.MarshalString())
		}
	}
	d.next, d.count = kept, count
}
