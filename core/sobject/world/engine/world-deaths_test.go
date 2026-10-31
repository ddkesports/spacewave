package sobject_world_engine

import (
	"slices"
	"testing"

	"github.com/s4wave/spacewave/db/block"
)

// TestWorldDeathsComposesAcceptedChain tries to make the transition table
// report a page from a path the named World did not take, or report anything
// across a gap or a forfeited commit.
func TestWorldDeathsComposesAcceptedChain(t *testing.T) {
	// ref returns a distinct ref for name.
	ref := func(name string) *block.BlockRef {
		r, err := block.BuildBlockRef([]byte(name), nil)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}

	// replaced returns a record of the named pages replaced.
	replaced := func(names ...string) *block.Replacements {
		out := &block.Replacements{}
		for _, name := range names {
			out.Replaced = append(out.Replaced, ref(name))
		}
		return out
	}

	// has reports whether dead lists the named page.
	has := func(dead *block.Replacements, name string) bool {
		return slices.ContainsFunc(dead.Replaced, ref(name).EqualsRef)
	}

	// A local fork from a to x replaced p, but replay accepted a to b to c,
	// which wrote q again after b replaced it.
	a, b, c, x := ref("a"), ref("b"), ref("c"), ref("x")
	d := newWorldDeaths(a)
	d.add(a, x, replaced("p"))
	d.add(a, b, replaced("q", "r"))
	d.add(b, c, &block.Replacements{Replaced: []*block.BlockRef{ref("s")}, Kept: []*block.BlockRef{ref("q")}})
	_, dead, ok := d.take(c)
	if !ok {
		t.Fatal("forfeited the accepted chain")
	}
	if has(dead, "p") || has(dead, "q") || !has(dead, "r") || !has(dead, "s") {
		t.Fatalf("composed %v, want r and s", dead.Replaced)
	}

	// A transition from a World never named leaves a gap.
	d.add(x, ref("y"), replaced("t"))
	if _, _, ok := d.take(ref("y")); ok {
		t.Fatal("composed across a gap")
	}

	// A forfeited commit on the chain forfeits the interval, and the next one
	// starts clean.
	e, f := ref("e"), ref("f")
	d.add(ref("y"), e, nil)
	d.add(e, f, replaced("u"))
	if _, _, ok := d.take(f); ok {
		t.Fatal("composed over a forfeited commit")
	}
	d.add(f, ref("g"), replaced("v"))
	if _, dead, ok := d.take(ref("g")); !ok || !has(dead, "v") || has(dead, "u") {
		t.Fatalf("next interval: %v ok=%v", dead, ok)
	}
}
