package sobject_world_engine

import (
	"context"
	"strings"

	"github.com/aperturerobotics/controllerbus/bus"
	"github.com/pkg/errors"
	"github.com/s4wave/spacewave/core/sobject"
	"github.com/s4wave/spacewave/db/block"
	"github.com/s4wave/spacewave/db/world"
)

// OpTypes resolves the World operation types an engine applies.
type OpTypes struct {
	// Lookup resolves built-in operations before the bus lookup, or is nil.
	Lookup world.LookupOp
	// Declared returns the plugins the World ws declares handle opTypeID, an
	// operation type neither Lookup nor the bus resolves. Replay stops at an
	// operation of a declared type until a handler for it resolves on the bus,
	// and rejects one of an undeclared type. It must read only ws, so every
	// replica decides alike. Nil declares none.
	Declared func(ctx context.Context, ws world.WorldState, opTypeID string) ([]string, error)
}

// pendingOpError reports an operation whose type the World declares a plugin
// handles, met before a handler for the type resolved.
type pendingOpError struct {
	// opTypeID is the operation type.
	opTypeID string
	// pluginIDs are the plugins the World declares handle it.
	pluginIDs []string
}

// Error returns the error message.
func (e *pendingOpError) Error() string {
	return "operation type " + e.opTypeID + " waits for a handler from plugin " + strings.Join(e.pluginIDs, ", ")
}

// replayGap is the input a replay stopped for, which a later replay needs to
// place the operation it stopped at. The zero value is no gap.
type replayGap struct {
	// missing is a block no connected peer could serve, or nil.
	missing *block.BlockRef
	// pending is an operation waiting for its declared handler, or nil.
	pending *pendingOpError
}

// isReplayGap reports whether err stops replay at its operation instead of
// deciding the operation: a block that is not available, or an operation
// whose declared handler has not resolved. Neither is a property of the
// operation, so rejecting it would split replicas.
func isReplayGap(err error) bool {
	var pending *pendingOpError
	return block.IsNotAvailable(err) || errors.As(err, &pending)
}

// gapOf returns the gap err reports, with missing as the block a read skipped.
func gapOf(err error, missing *block.BlockRef) replayGap {
	var gap replayGap
	if block.IsNotAvailable(err) {
		gap.missing = missing
	}
	_ = errors.As(err, &gap.pending)
	return gap
}

// empty reports whether the gap names no input to wait for.
func (g replayGap) empty() bool {
	return g.missing == nil && g.pending == nil
}

// declaredLookup wraps lookupOp so that an operation type it does not
// resolve, and that ws declares a plugin handles, fails with a pendingOpError
// instead of resolving to nothing.
func (c *Controller) declaredLookup(lookupOp world.LookupOp, ws world.WorldState) world.LookupOp {
	declared := c.getOpTypes().Declared
	if declared == nil {
		return lookupOp
	}
	return func(ctx context.Context, opTypeID string) (world.Operation, error) {
		// Resolve the operation, then check an unresolved type's declaration.
		op, err := lookupOp(ctx, opTypeID)
		if op != nil || err != nil {
			return op, err
		}
		pluginIDs, err := declared(ctx, ws, opTypeID)
		if err != nil || len(pluginIDs) == 0 {
			return nil, err
		}
		return nil, &pendingOpError{opTypeID: opTypeID, pluginIDs: pluginIDs}
	}
}

// waitGap waits until the input gap names is available: a peer serves the
// missing block, or a handler for the pending operation type resolves on the
// bus. While it waits for a handler it asks for the declaring plugins with a
// LoadOpPlugins directive; a handler resolves once one of them runs and
// registers it.
func (c *Controller) waitGap(ctx context.Context, so sobject.SharedObject, gap replayGap) error {
	// Fetch the block with a read that waits for a peer.
	if gap.missing != nil {
		_, found, err := so.GetBlockStore().GetBlock(ctx, gap.missing)
		if err != nil {
			return err
		}
		if !found {
			return errors.Wrap(block.ErrNotFound, gap.missing.MarshalString())
		}
	}

	// Wait for a handler of the pending operation type.
	if gap.pending != nil {
		if c.conf.GetDisableLookup() {
			<-ctx.Done()
			return context.Cause(ctx)
		}

		// Ask the plugin runtime holders to run the declaring plugins.
		_, loadRef, err := c.bus.AddDirective(ctx, NewLoadOpPlugins(c.engineID, gap.pending.pluginIDs), nil)
		if err != nil {
			return err
		}
		defer loadRef.Release()

		// Wait for a lookup value that resolves the operation type. Other
		// values, such as the built-in operations, resolve nothing for it.
		opTypeID := gap.pending.opTypeID
		_, _, ref, err := bus.ExecWaitValue[world.LookupWorldOpValue](
			ctx,
			c.bus,
			world.NewLookupWorldOp(opTypeID, c.engineID),
			nil,
			nil,
			func(lookup world.LookupWorldOpValue) (bool, error) {
				op, err := lookup(ctx, opTypeID)
				return op != nil && err == nil, nil
			},
		)
		if err != nil {
			return err
		}
		ref.Release()
	}
	return nil
}
