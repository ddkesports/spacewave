//go:build !tinygo

package provider_local

import (
	"context"
	"crypto/tls"
	"errors"
	"github.com/s4wave/spacewave/db/kvtx"
	"net"
	"strconv"

	account_settings "github.com/s4wave/spacewave/core/account/settings"
	block_store_ftps "github.com/s4wave/spacewave/db/block/store/ftps"
	block_store_s3 "github.com/s4wave/spacewave/db/block/store/s3"
	"github.com/sirupsen/logrus"
)

// errCheckListed stops the FTPS check's listing after its first object.
var errCheckListed = errors.New("listed")

// buildFTPSClient logs in from the location's netrc file and builds a client.
// A nil tlsConf verifies the server against the system roots. It connects on
// its first call.
func buildFTPSClient(location *account_settings.FtpsLocation, tlsConf *tls.Config) (*block_store_ftps.Client, error) {
	logins, err := block_store_ftps.ReadNetrc(location.GetNetrcPath(), location.GetHost())
	if err != nil {
		return nil, err
	}
	addr := location.GetHost()
	if port := location.GetPort(); port != 0 {
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

// buildFTPSBlockStore opens the packfile store on the location's directory.
// The store keeps every packfile: nothing compacts or deletes one, and it
// reads the location's prefix as given. index, when not nil, is the local KV
// that holds the store's entry cache and block location index.
func buildFTPSBlockStore(le *logrus.Entry, location *account_settings.FtpsLocation, tlsConf *tls.Config, index kvtx.Store) (backendStore, error) {
	client, err := buildFTPSClient(location, tlsConf)
	if err != nil {
		return nil, err
	}
	return block_store_s3.NewPackStore(le, client, location.GetRoot(), location.GetPrefix(), &block_store_s3.PackStoreOptions{
		KeepPacks: true,
		Index:     index,
	}), nil
}

// CheckFTPSLocation logs in and lists the location's prefix, which writes
// nothing into the packfile directory.
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
