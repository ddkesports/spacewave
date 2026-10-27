package provider_local

import (
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
