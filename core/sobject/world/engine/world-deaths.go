package sobject_world_engine

import (
	"slices"
	"sync"

	"github.com/s4wave/spacewave/db/block"
)

// maxWorldTransitions bounds the transitions worldDeaths holds between two
// compositions. An engine that never names a World, such as one installing
// another participant's checkpoints, reaches it and forfeits instead of
// growing.
const maxWorldTransitions = 4096

// maxWorldPuts bounds the World page puts worldDeaths holds between two
// compositions. Past it the puts are dropped, so the pages they found count as
// created, which leaks.
const maxWorldPuts = 1 << 16

// worldTransition is one World commit: the root it made and what its writes
// did.
type worldTransition struct {
	// to is the root the commit made.
	to *block.BlockRef
	// step is what the commit's writes did, or nil when the commit forfeited
	// its record.
	step *block.ReplacementStep
}

// worldPut is a publication that put World pages: its receipt, and the key of
// each entry whose put the store may report found, by index.
type worldPut struct {
	receipt *block.PublicationReceipt
	keys    map[int]string
}

// newWorldPut returns the puts of pages among entries, the entries of the
// publication receipt resolves. A counted page never reports found.
func newWorldPut(receipt *block.PublicationReceipt, entries, pages []*block.PutBatchEntry) worldPut {
	// Index the pages, then key each one the receipt reports by position.
	isPage := make(map[*block.PutBatchEntry]struct{}, len(pages))
	for _, e := range pages {
		isPage[e] = struct{}{}
	}
	keys := make(map[int]string, len(pages))
	for i, e := range entries {
		if _, ok := isPage[e]; ok && !e.Tombstone && !e.Counted {
			keys[i] = e.Ref.MarshalString()
		}
	}
	return worldPut{receipt: receipt, keys: keys}
}

// pendingNet is a named World's interval waiting for the puts that stored its
// pages: their receipts report which pages the store already held, which the
// record needs to net. It is used under the writer lock.
type pendingNet struct {
	// steps are the steps from the last named World to this one, joined only
	// when netting, off the writer lock.
	steps block.ReplacementSteps
	// puts are the publications that put the interval's pages.
	puts []worldPut
	// sealed is set once the naming publication joined puts.
	sealed bool
	// encoded closes once the record is encoded off the writer lock, after
	// which the fields below are set.
	encoded chan struct{}
	// dead is what died in the interval.
	dead *block.Replacements
	// blocks and record are the encoded record's blocks and first block.
	blocks []*block.PutBatchEntry
	record *block.BlockRef
	// err is the encoding error, if any.
	err error
}

// seal adds the naming publication's puts, or none when it was not
// submitted, after which the interval waits only on its receipts.
func (n *pendingNet) seal(put *worldPut) {
	if put != nil {
		n.puts = append(n.puts, *put)
	}
	n.sealed = true
}

// ready reports whether the interval is sealed and each put resolved.
func (n *pendingNet) ready() bool {
	if !n.sealed {
		return false
	}
	for _, put := range n.puts {
		select {
		case <-put.receipt.Done():
		default:
			return false
		}
	}
	return true
}

// poll starts encoding the interval's record as seq from from to to once
// every put resolved, and reports whether the encoded record is ready. Netting
// walks every page the interval wrote, tens of milliseconds for a long
// interval, so it runs off the writer lock and no commit waits on it; it
// waits on nothing, so it always ends. Call it under the writer lock.
func (n *pendingNet) poll(seq uint64, from, to *block.BlockRef) bool {
	// Start the encoding once every put resolved.
	if n.encoded == nil {
		if !n.ready() {
			return false
		}
		n.encoded = make(chan struct{})
		go func() {
			defer close(n.encoded)
			n.dead = n.net()
			n.blocks, n.record, n.err = block.EncodeDeadlist(seq, from, to, n.dead)
		}()
		return false
	}

	// Report whether it finished.
	select {
	case <-n.encoded:
		return true
	default:
		return false
	}
}

// net returns what died in the interval. A page a successful put reported the
// store held is found; a put that failed or did not report counts its pages
// created, which leaks. Call it once ready.
func (n *pendingNet) net() *block.Replacements {
	found := make(map[string]struct{})
	for _, put := range n.puts {
		if put.receipt.Err() != nil {
			continue
		}
		existed := put.receipt.Existed()
		for i, key := range put.keys {
			if i < len(existed) && existed[i] {
				found[key] = struct{}{}
			}
		}
	}
	return n.steps.Join().Net(found)
}

// worldDeaths gathers the World transitions the engine's block engines commit,
// local writes and replay alike, and the publications that put World pages,
// and joins the chain from the last named World to the next into one step.
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
	// puts are the publications that put World pages since the last
	// composition.
	puts []worldPut
	// putKeys is the number of keys in puts.
	putKeys int
}

// newWorldDeaths starts gathering at the World root from.
func newWorldDeaths(from *block.BlockRef) *worldDeaths {
	return &worldDeaths{from: from.Clone(), next: make(map[string][]worldTransition)}
}

// add records a commit's transition. It is the World block engines'
// TransitionSink.
func (d *worldDeaths) add(from, to *block.BlockRef, step *block.ReplacementStep) {
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
	d.next[key] = append(d.next[key], worldTransition{to: to.Clone(), step: step})
	d.count++
}

// addPut records a publication that put World pages, dropping it when the
// puts are full.
func (d *worldDeaths) addPut(put worldPut) {
	// Skip a publication that put no pages.
	if len(put.keys) == 0 {
		return
	}

	// Add it unless the puts are full.
	d.mtx.Lock()
	defer d.mtx.Unlock()
	if d.putKeys+len(put.keys) > maxWorldPuts {
		return
	}
	d.puts = append(d.puts, put)
	d.putKeys += len(put.keys)
}

// take joins the steps from the last named World, from, to the World at root
// with the puts since, and starts the next interval at root. net is nil when
// no chain of recorded transitions reaches root or one on it forfeited; the
// interval then leaks. from is nil when the interval forfeited before root was
// named. Only the transitions root reaches stay.
func (d *worldDeaths) take(root *block.BlockRef) (from *block.BlockRef, net *pendingNet) {
	// Find a path from the last named World to root and join it.
	d.mtx.Lock()
	defer d.mtx.Unlock()
	from = d.from
	if from != nil {
		if steps := d.compose(d.from, root); steps != nil {
			net = &pendingNet{steps: steps, puts: d.puts}
		}
	}

	// Keep the transitions root reaches and start there.
	d.prune(root)
	d.from = root.Clone()
	d.puts, d.putKeys = nil, 0
	return from, net
}

// compose finds a path of transitions from from to root, breadth first, and
// returns their steps in order, or nil when none does. Assumes mtx is locked.
func (d *worldDeaths) compose(from, root *block.BlockRef) block.ReplacementSteps {
	// An unchanged World lost nothing.
	if from.EqualsRef(root) {
		return block.ReplacementSteps{{From: from, To: root}}
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
		return nil
	}

	// Collect the path back from root, then order it from the start.
	var path block.ReplacementSteps
	for key := goal; key != start; key = into[key].prev {
		if into[key].tr.step == nil {
			return nil
		}
		path = append(path, into[key].tr.step)
	}
	slices.Reverse(path)
	return path
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
