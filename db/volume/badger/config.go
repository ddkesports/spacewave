package volume_badger

import (
	"github.com/aperturerobotics/controllerbus/config"
	bdb "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/pkg/errors"
)

// ConfigID is the id attached to the config objects.
var ConfigID = ControllerID

const (
	// defaultMemTableSize raises Badger's transaction limit, which is 15% of a
	// memtable, since Bolt has none: 256 MiB holds about 38 MiB of keys, small
	// values and value pointers in one transaction.
	defaultMemTableSize = 256 << 20
	// defaultNumMemtables bounds the memtables to about 1 GiB with the size
	// above.
	defaultNumMemtables = 3
	// defaultValueThreshold keeps blocks in the value log, where compaction
	// never rewrites them and they count only as a pointer against the
	// transaction limit. Small records stay in the tree.
	defaultValueThreshold = 1 << 10
	// defaultIndexCacheSize bounds table indexes and bloom filters, which
	// otherwise stay in the heap for every table.
	defaultIndexCacheSize = 256 << 20
)

// BuildBadgerOptions builds badger options from the config.
func (c *Config) BuildBadgerOptions() (*bdb.Options, error) {
	// Require a storage directory for the Badger database.
	if c.GetDir() == "" {
		return nil, errors.New("db dir cannot be empty")
	}

	// Resolve the table and value log directories from the configuration.
	o := bdb.DefaultOptions(c.GetDir())
	if vd := c.GetValueDir(); vd != "" {
		o.ValueDir = vd
	} else {
		o.ValueDir = o.Dir
	}

	// We use a write mutex, so conflict checking is unnecessary.
	o.DetectConflicts = false

	// Stored blocks are encrypted, so compression spends CPU for nothing. With
	// it off the tables read through the page cache, as a Bolt file does.
	o.Compression = options.None
	o.BlockCacheSize = 0
	o.IndexCacheSize = defaultIndexCacheSize

	// Size the memtables and value log for block storage.
	o.MemTableSize = defaultMemTableSize
	o.NumMemtables = defaultNumMemtables
	o.ValueThreshold = defaultValueThreshold

	// Configure how many historical versions Badger retains.
	if nvc := c.GetNumVersionsToKeep(); nvc != 0 {
		o.NumVersionsToKeep = int(nvc)
	}

	// Tune Badger table growth and the number of storage levels.
	if bts := c.GetBaseTableSize(); bts != 0 {
		o.BaseTableSize = int64(bts) //nolint:gosec
	}
	if lsm := c.GetLevelSizeMultiplier(); lsm != 0 {
		o.LevelSizeMultiplier = int(lsm)
	}
	if ml := c.GetMaxLevels(); ml != 0 {
		o.MaxLevels = int(ml)
	}

	// Tune in-memory buffering and which values remain in the tables.
	if vt := c.GetValueThreshold(); vt != 0 {
		o.ValueThreshold = int64(vt) //nolint:gosec
	}
	if nmt := c.GetNumMemtables(); nmt != 0 {
		o.NumMemtables = int(nmt)
	}

	// Tune level zero pressure and the base storage level size.
	if nlzt := c.GetNumLevelZeroTables(); nlzt != 0 {
		o.NumLevelZeroTables = int(nlzt)
	}
	if nlzts := c.GetNumLevelZeroTablesStall(); nlzts != 0 {
		o.NumLevelZeroTablesStall = int(nlzts)
	}
	if los := c.GetBaseLevelSize(); los != 0 {
		o.BaseLevelSize = int64(los) //nolint:gosec
	}

	// Bound Badger value log files by size and entry count.
	if vlfs := c.GetValueLogFileSize(); vlfs != 0 {
		o.ValueLogFileSize = int64(vlfs) //nolint:gosec
	}
	if vlme := c.GetValueLogMaxEntries(); vlme != 0 {
		o.ValueLogMaxEntries = vlme
	}

	// Configure the Badger background compaction concurrency.
	if nc := c.GetNumCompactors(); nc != 0 {
		o.NumCompactors = int(nc)
	}

	return &o, nil
}

// Validate validates the configuration.
// This is a cursory validation to see if the values "look correct."
func (c *Config) Validate() error {
	if _, err := c.BuildBadgerOptions(); err != nil {
		return err
	}

	return nil
}

// GetConfigID returns the unique string for this configuration type.
// This string is stored with the encoded config.
func (c *Config) GetConfigID() string {
	return ControllerID
}

// EqualsConfig checks if the config is equal to another.
func (c *Config) EqualsConfig(other config.Config) bool {
	return config.EqualsConfig[*Config](c, other)
}

// GetDebugVals returns the directive arguments as key/value pairs.
// This should be something like param1="test", param2="test".
// This is not necessarily unique, and is primarily intended for display.
func (c *Config) GetDebugVals() config.DebugValues {
	vals := make(config.DebugValues)
	if dir := c.GetDir(); dir != "" {
		vals["dir"] = []string{dir}
	}
	if valueDir := c.GetValueDir(); valueDir != "" {
		vals["value-dir"] = []string{valueDir}
	}
	return vals
}

// _ is a type assertion
var _ config.Config = (*Config)(nil)

// _ is a type assertion
var _ config.Debuggable = (*Config)(nil)
