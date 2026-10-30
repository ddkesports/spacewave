package s4wave_mount

import (
	"strings"
	"testing"

	s4wave_provider_core "github.com/s4wave/spacewave/core/provider"
	s4wave_sobject_core "github.com/s4wave/spacewave/core/sobject"
	s4wave_space_core "github.com/s4wave/spacewave/core/space"
)

// TestResolveSpaceIDFromList resolves IDs, names and the sole Space, and
// rejects Spaces the list does not hold.
func TestResolveSpaceIDFromList(t *testing.T) {
	// Build a two-Space list and a one-Space list.
	two := []*s4wave_space_core.SpaceSoListEntry{
		testSpaceListEntry("01other", "Other"),
		testSpaceListEntry("01agents", "Agent clients"),
	}
	one := two[1:]

	// Check each selection against its expected ID or error.
	cases := []struct {
		space  string
		spaces []*s4wave_space_core.SpaceSoListEntry
		want   string
		err    string
	}{
		{space: "Agent clients", spaces: two, want: "01agents"},
		{space: "01agents", spaces: one, want: "01agents"},
		{space: "", spaces: one, want: "01agents"},
		{space: "", spaces: two, err: "multiple spaces found"},
		{space: "missing", spaces: one, err: `space "missing" not found`},
		{space: "", err: "no spaces found"},
	}
	for _, tc := range cases {
		got, err := ResolveSpaceIDFromList(tc.space, tc.spaces)
		if tc.err != "" {
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("%q: got %v, want an error containing %q", tc.space, err, tc.err)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Fatalf("%q: got %q, %v, want %q", tc.space, got, err, tc.want)
		}
	}
}

// testSpaceListEntry builds a Space list entry with an ID and display name.
func testSpaceListEntry(id, name string) *s4wave_space_core.SpaceSoListEntry {
	return &s4wave_space_core.SpaceSoListEntry{
		Entry: &s4wave_sobject_core.SharedObjectListEntry{
			Ref: &s4wave_sobject_core.SharedObjectRef{
				ProviderResourceRef: &s4wave_provider_core.ProviderResourceRef{Id: id},
			},
		},
		SpaceMeta: &s4wave_space_core.SpaceSoMeta{Name: name},
	}
}
