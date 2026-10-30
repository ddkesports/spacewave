package space_world

import (
	"context"
	"slices"

	manifest "github.com/s4wave/spacewave/bldr/manifest"
	manifest_world "github.com/s4wave/spacewave/bldr/manifest/world"
	"github.com/s4wave/spacewave/db/block"
	"github.com/s4wave/spacewave/db/world"
	world_types "github.com/s4wave/spacewave/db/world/types"
)

// InstalledPluginManifests returns the manifest metadata of each plugin the
// Space settings in ws install, keyed by plugin ID. A plugin pinned by an
// installation reads its newest pin that resolves; any other plugin reads the
// highest revision of its manifest among the World's manifests. A plugin with
// no readable manifest is left out. It reads only ws, so every replica of the
// same World gets the same result; a block that is not available returns its
// error rather than leaving a manifest out.
func InstalledPluginManifests(ctx context.Context, ws world.WorldState) (map[string]*manifest.ManifestMeta, error) {
	// Read the installed plugins.
	settings, err := LookupSpaceSettingsBody(ctx, ws)
	if err != nil || len(settings.GetPluginIds()) == 0 {
		return nil, err
	}
	out := make(map[string]*manifest.ManifestMeta, len(settings.GetPluginIds()))

	// Read each pinned plugin's newest pin that resolves.
	unpinned := make(map[string]struct{})
	for _, pluginID := range settings.GetPluginIds() {
		keys := settings.GetPluginInstallations()[pluginID].GetManifestKeys()
		if len(keys) == 0 {
			unpinned[pluginID] = struct{}{}
			continue
		}
		for _, key := range keys {
			ref, err := LookupSpacePluginManifest(ctx, ws, pluginID, key)
			if block.IsNotAvailable(err) {
				return nil, err
			}
			if err == nil {
				out[pluginID] = ref.GetMeta()
				break
			}
		}
	}
	if len(unpinned) == 0 {
		return out, nil
	}

	// Read the highest revision of each other plugin's manifest.
	keys, err := world_types.ListObjectsWithType(ctx, ws, manifest_world.ManifestTypeID)
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		m, _, err := manifest_world.LookupManifest(ctx, ws, key)
		if block.IsNotAvailable(err) {
			return nil, err
		}
		meta := m.GetMeta()
		if _, ok := unpinned[meta.GetManifestId()]; err != nil || !ok {
			continue
		}
		if prev, ok := out[meta.GetManifestId()]; !ok || meta.GetRev() > prev.GetRev() {
			out[meta.GetManifestId()] = meta
		}
	}
	return out, nil
}

// DeclaredOpPlugins returns the sorted IDs of the plugins the Space in ws
// installs whose manifests declare the World operation type opTypeID, as
// InstalledPluginManifests reads them.
func DeclaredOpPlugins(ctx context.Context, ws world.WorldState, opTypeID string) ([]string, error) {
	// Read the installed plugins' manifests.
	metas, err := InstalledPluginManifests(ctx, ws)
	if err != nil {
		return nil, err
	}

	// Select the plugins whose manifests declare the operation type.
	var pluginIDs []string
	for pluginID, meta := range metas {
		if slices.Contains(meta.GetOpTypeIds(), opTypeID) {
			pluginIDs = append(pluginIDs, pluginID)
		}
	}
	slices.Sort(pluginIDs)
	return pluginIDs, nil
}
