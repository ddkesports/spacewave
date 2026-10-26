package sobject_world_engine

import "context"

// CommitObserver receives the stage times of each World write when its Commit
// returns, whether or not the commit succeeded. It runs on the committing
// goroutine after the writer lock is released, so it must return quickly and
// must not write to the World.
type CommitObserver func(CommitTimes)

// commitObserverKey is the context key of the CommitObserver.
type commitObserverKey struct{}

// WithCommitObserver returns a context whose World writes report their stage
// times to obs. Pass it to NewTransaction; the write keeps the observer it
// opened with.
func WithCommitObserver(ctx context.Context, obs CommitObserver) context.Context {
	return context.WithValue(ctx, commitObserverKey{}, obs)
}

// commitObserverFromContext returns the CommitObserver of ctx, or nil.
func commitObserverFromContext(ctx context.Context) CommitObserver {
	obs, _ := ctx.Value(commitObserverKey{}).(CommitObserver)
	return obs
}
