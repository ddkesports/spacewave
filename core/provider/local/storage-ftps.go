//go:build !tinygo

package provider_local

import (
	"cmp"
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strconv"

	"github.com/s4wave/spacewave/db/kvtx"

	account_settings "github.com/s4wave/spacewave/core/account/settings"
	block_store_ftps "github.com/s4wave/spacewave/db/block/store/ftps"
	block_store_s3 "github.com/s4wave/spacewave/db/block/store/s3"
	block_store_sftp "github.com/s4wave/spacewave/db/block/store/sftp"
	"github.com/sirupsen/logrus"
)

// errCheckListed stops the FTPS check's listing after its first object.
var errCheckListed = errors.New("listed")

// packServer is the object store of an FTPS or SFTP server, which closes its
// connections.
type packServer interface {
	block_store_s3.ObjectStore
	// Close closes the connections; a later call connects again.
	Close()
}

// buildFTPSClient logs in from the location's netrc file and builds a client
// of the location's protocol: SFTP when it sets sftp, else FTPS. A nil tlsConf
// verifies an FTPS server against the system roots. It connects on its first
// call.
func buildFTPSClient(location *account_settings.FtpsLocation, tlsConf *tls.Config) (packServer, error) {
	// Read the logins from the netrc file.
	logins, err := block_store_ftps.ReadNetrc(location.GetNetrcPath(), location.GetHost())
	if err != nil {
		return nil, err
	}
	addr := location.GetHost()
	port := location.GetPort()
	if location.GetSftp() {
		// SFTP carries many requests on each connection, so MaxConns is the
		// number of connections and the logins do not add more.
		return block_store_sftp.NewClient(block_store_sftp.Config{
			Addr:     net.JoinHostPort(addr, strconv.FormatUint(uint64(cmp.Or(port, 22)), 10)),
			User:     logins[0].User,
			Password: logins[0].Password,
			HostKey:  location.GetHostKey(),
			Conns:    int(location.GetMaxConns()),
		})
	}
	if port != 0 {
		addr = net.JoinHostPort(addr, strconv.FormatUint(uint64(port), 10))
	}
	return block_store_ftps.NewClient(block_store_ftps.Config{
		Addr:          addr,
		Logins:        logins,
		TLSConfig:     tlsConf,
		MaxSessions:   int(location.GetMaxConns()),
		ConnsPerLogin: int(location.GetConnsPerLogin()),
	}), nil
}

// ftpsReadWindow is the fewest bytes a block miss reads from a pack: one SFTP
// packet. The packs hold blocks in key order, which is hash order, so the
// bytes beside a block belong to unrelated blocks and a wider read only adds
// transfer time to the miss.
const ftpsReadWindow = 32 << 10

// buildFTPSBlockStore opens the packfile store on the location's directory,
// over FTPS or, when the location sets sftp, SFTP.
// The store keeps every packfile: nothing compacts or deletes one, and it
// reads the location's prefix as given. index, when not nil, is the local KV
// that holds the store's entry cache and block location index.
func buildFTPSBlockStore(le *logrus.Entry, location *account_settings.FtpsLocation, tlsConf *tls.Config, index kvtx.Store) (backendStore, error) {
	client, err := buildFTPSClient(location, tlsConf)
	if err != nil {
		return nil, err
	}
	return block_store_s3.NewPackStore(le, client, location.GetRoot(), location.GetPrefix(), &block_store_s3.PackStoreOptions{
		KeepPacks:  true,
		Index:      index,
		ReadWindow: ftpsReadWindow,
	}), nil
}

// CheckFTPSLocation logs in to an FTPS or SFTP location and lists its prefix,
// which writes nothing into the packfile directory.
func CheckFTPSLocation(ctx context.Context, location *account_settings.FtpsLocation) *block_store_s3.CheckResult {
	client, err := buildFTPSClient(location, nil)
	if err == nil {
		defer client.Close()
		err = client.ListObjects(ctx, location.GetRoot(), location.GetPrefix(), func(string, int64) error { return errCheckListed })
		if errors.Is(err, errCheckListed) {
			err = nil
		}
	}
	if err != nil {
		return &block_store_s3.CheckResult{Outcome: block_store_s3.CheckOutcome_CHECK_OUTCOME_FAILED, Detail: err.Error()}
	}
	return &block_store_s3.CheckResult{Outcome: block_store_s3.CheckOutcome_CHECK_OUTCOME_OK}
}
