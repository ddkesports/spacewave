package provider_local

import (
	"context"

	"github.com/s4wave/spacewave/net/peer"
)

// AccessSessionAccount returns the running account whose Session transport
// runs as peer id, and a release for the reference it adds. It returns nil
// when no running account's transport runs as id, such as before the transport
// starts. A controller on a Session's bus knows only the Session peer, and the
// account is the one that hosts that Session.
func (p *Provider) AccessSessionAccount(id peer.ID) (*ProviderAccount, func()) {
	// Visit each account with a reference, so the lookup starts none.
	for _, key := range p.accountRc.GetKeys() {
		ref, tkr, existed := p.accountRc.AddKeyRef(key)
		if !existed {
			ref.Release()
			continue
		}

		// Keep the account whose running transport has the Session peer.
		acc := tkr.accCtr.GetValue()
		if acc != nil {
			if st := acc.GetSessionTransport(); st != nil && st.GetPeerID() == id {
				return acc, ref.Release
			}
		}
		ref.Release()
	}
	return nil, nil
}

// WaitSessionAccount waits for a running account whose Session transport runs
// as peer id, and returns it as AccessSessionAccount does.
func (p *Provider) WaitSessionAccount(ctx context.Context, id peer.ID) (*ProviderAccount, func(), error) {
	for {
		// Take the wait channel before looking, so a start between the look
		// and the wait still wakes this waiter.
		var waitCh <-chan struct{}
		p.sessionBcast.HoldLock(func(_ func(), getWaitCh func() <-chan struct{}) {
			waitCh = getWaitCh()
		})
		if acc, release := p.AccessSessionAccount(id); acc != nil {
			return acc, release, nil
		}

		// Wait for an account or a transport to start or stop.
		select {
		case <-ctx.Done():
			return nil, nil, context.Cause(ctx)
		case <-waitCh:
		}
	}
}

// wakeSessionWaiters wakes WaitSessionAccount after an account or its Session
// transport starts or stops.
func (p *Provider) wakeSessionWaiters() {
	p.sessionBcast.HoldLock(func(broadcast func(), _ func() <-chan struct{}) {
		broadcast()
	})
}
