package world

import (
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"
)

// TestChangeIndexCandidates checks which indexed ReadSets a ChangeSet names.
func TestChangeIndexCandidates(t *testing.T) {
	// Index a key read, a pattern read, a prefix read and a plain filter.
	keyed, linked, prefixed := NewReadSet(), NewReadSet(), NewReadSet()
	keyed.addKeys("obj/a")
	linked.addPatterns(NewGraphQuad("<obj/b>", "", "", ""))
	prefixed.addPrefix("obj/")
	x := NewChangeIndex[string]()
	for name, set := range map[string]*ReadSet{"keyed": keyed, "linked": linked, "prefixed": prefixed} {
		x.Add(name, readSetInterest(set))
	}
	x.Add("filter", NewChangeInterest(func(*ChangeSet) bool { return false }))

	// Each change names its readers and the unindexed watchers.
	cases := []struct {
		changes *ChangeSet
		want    []string
	}{
		{&ChangeSet{Keys: []string{"obj/a"}}, []string{"filter", "keyed", "prefixed"}},
		{&ChangeSet{Keys: []string{"obj/b"}}, []string{"filter", "linked", "prefixed"}},
		{&ChangeSet{Quads: []GraphQuad{NewGraphQuad("<obj/b>", "<p>", "<obj/c>", "")}}, []string{"filter", "linked", "prefixed"}},
		{&ChangeSet{Keys: []string{"other"}}, []string{"filter", "prefixed"}},
		{NewUnknownChangeSet(), []string{"filter", "keyed", "linked", "prefixed"}},
	}
	for i, c := range cases {
		if got := slices.Sorted(slices.Values(x.Candidates(c.changes))); !slices.Equal(got, c.want) {
			t.Errorf("case %d: candidates %q, want %q", i, got, c.want)
		}
	}

	// Removing every watcher leaves no name behind.
	for _, name := range []string{"keyed", "linked", "prefixed", "filter"} {
		x.Remove(name)
	}
	if x.Len() != 0 || len(x.keys) != 0 || len(x.terms) != 0 || len(x.all) != 0 {
		t.Fatalf("index holds %d watchers, %d keys, %d terms, %d unindexed after removal", x.Len(), len(x.keys), len(x.terms), len(x.all))
	}
}

// TestChangeIndexSound checks on random reads and changes that every ReadSet
// a ChangeSet touches is among the index's candidates.
func TestChangeIndexSound(t *testing.T) {
	// Index random ReadSets over a small pool of keys and quad values.
	rng := rand.New(rand.NewPCG(1, 2))
	pick := func() string { return "<k" + strconv.Itoa(rng.IntN(12)) + ">" }
	maybe := func() string {
		if rng.IntN(2) == 0 {
			return ""
		}
		return pick()
	}
	x := NewChangeIndex[int]()
	sets := make([]*ReadSet, 200)
	for i := range sets {
		sets[i] = randomReadSet(rng, pick, maybe)
		x.Add(i, readSetInterest(sets[i]))
	}

	// Each random ChangeSet names every set it touches.
	for range 2000 {
		changes := &ChangeSet{}
		for range rng.IntN(3) {
			changes.Keys = append(changes.Keys, pick())
		}
		for range rng.IntN(3) {
			changes.Quads = append(changes.Quads, NewGraphQuad(pick(), pick(), pick(), maybe()))
		}
		candidates := x.Candidates(changes)
		for i, set := range sets {
			if set.Touched(changes) && !slices.Contains(candidates, i) {
				t.Fatalf("set %d touched by %+v is not a candidate", i, changes)
			}
		}
	}
}

// randomReadSet records a few random reads of every kind but conservative ones.
func randomReadSet(rng *rand.Rand, pick, maybe func() string) *ReadSet {
	// Read some keys and look up some quad patterns.
	set := NewReadSet()
	for range rng.IntN(3) {
		set.addKeys(pick())
	}
	for range rng.IntN(3) {
		set.addPatterns(NewGraphQuad(maybe(), maybe(), maybe(), maybe()))
	}

	// Iterate a prefix or read the whole graph now and then.
	switch rng.IntN(20) {
	case 0:
		set.addPrefix("<k1")
	case 1:
		set.markGraph()
	}
	return set
}

// readSetInterest names the reads of set over a filter that tests them.
func readSetInterest(set *ReadSet) *ChangeInterest {
	interest := &ChangeInterest{filter: set.Touched}
	set.nameReads(interest)
	return interest
}
