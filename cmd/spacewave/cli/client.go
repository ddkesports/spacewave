//go:build !js

package spacewave_cli

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"slices"

	"github.com/aperturerobotics/cli"
	"github.com/aperturerobotics/starpc/srpc"
	"github.com/aperturerobotics/util/gitroot"
	"github.com/pkg/errors"
	cli_entrypoint "github.com/s4wave/spacewave/bldr/cli/entrypoint"
	resource "github.com/s4wave/spacewave/bldr/resource"
	resource_client "github.com/s4wave/spacewave/bldr/resource/client"
	"github.com/s4wave/spacewave/core/daemon"
	s4wave_account "github.com/s4wave/spacewave/sdk/account"
	"github.com/s4wave/spacewave/sdk/cli/runner"
	s4wave_mount "github.com/s4wave/spacewave/sdk/mount"
	s4wave_provider "github.com/s4wave/spacewave/sdk/provider"
	s4wave_provider_local "github.com/s4wave/spacewave/sdk/provider/local"
	s4wave_provider_spacewave "github.com/s4wave/spacewave/sdk/provider/spacewave"
	s4wave_root "github.com/s4wave/spacewave/sdk/root"
	s4wave_session "github.com/s4wave/spacewave/sdk/session"
	s4wave_sobject "github.com/s4wave/spacewave/sdk/sobject"
	s4wave_space "github.com/s4wave/spacewave/sdk/space"
	sdk_engine "github.com/s4wave/spacewave/sdk/world/engine"
)

// projectID is the canonical project identifier for native state.
const projectID = "spacewave"

// defaultStatePath is the default path for daemon state.
var defaultStatePath = cli_entrypoint.DefaultStatePath(projectID)

// statePathEnvVars are the environment variables that override the daemon state path.
var statePathEnvVars = cli_entrypoint.StatePathEnvVars(projectID)

// socketPathEnvVars are the environment variables that override the daemon socket path directly.
// When set, the CLI dials this exact socket path without joining a state directory.
var socketPathEnvVars = []string{"SPACEWAVE_SOCKET_PATH"}

// socketName is the name of the Unix socket within the state path.
const socketName = daemon.SocketName

// sdkClient wraps the Resource SDK connection to a running daemon. A
// successfully connected client does not retain the daemon process handle.
type sdkClient struct {
	// conn carries connection-scoped daemon control operations.
	conn net.Conn
	// srpc exposes daemon services to command adapters.
	srpc srpc.Client
	// resClient retains the initialized Resource stream.
	resClient *resource_client.Client
	// root is the adopted root resource reference.
	root *s4wave_root.Root
	// shared owns native transport cleanup; nil for in-process clients.
	shared *daemon.Client
	// release frees what carries a remote client: its peer transport, Session
	// and local daemon client. Nil for local clients.
	release func()
}

// nativeClientFactory connects command runners to the shared native daemon.
type nativeClientFactory struct{}

// nativeClient adapts the daemon client to the shared command runner.
type nativeClient struct {
	// client retains the CLI resource facade.
	client *sdkClient
}

// nativeSession adapts Session watches to command runner streams.
type nativeSession struct {
	*s4wave_session.Session
}

var (
	// connectDaemonDial selects the socket transport for command wiring tests.
	connectDaemonDial = func(ctx context.Context, sockPath string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", sockPath)
	}
	// connectDaemonBuildClient constructs command SDK facades.
	connectDaemonBuildClient = buildSDKClient
	// connectDaemonStart transfers startup custody through the shared launcher.
	connectDaemonStart = daemon.StartProcess
)

// connectDaemon connects to the running daemon via Unix socket.
// It joins statePath with the canonical socket name and never starts or
// takes over a daemon implicitly.
func connectDaemon(ctx context.Context, statePath string) (*sdkClient, error) {
	return connectDaemonAtSocket(ctx, filepath.Join(statePath, socketName))
}

// connectDaemonWithAutostart uses the shared native startup contract. Once
// readiness transfers custody, a client failure cannot stop the daemon.
func connectDaemonWithAutostart(ctx context.Context, statePath string) (*sdkClient, error) {
	// Reuse daemon arbitration before constructing the CLI's SDK facade.
	conn, err := daemon.NewConnector(connectDaemonDial, connectDaemonStart).Dial(ctx, statePath, "")
	if err != nil {
		return nil, err
	}
	return connectDaemonBuildClient(ctx, conn)
}

// connectDaemonAtSocket dials an existing daemon socket at the exact given
// path. It never autostarts a daemon. Intended for connecting to a running
// desktop app or any pre-existing socket the caller has resolved by path.
// Dial failure is reported as a hard error with actionable guidance: the
// caller asked for connect-only semantics, so silently spawning a new
// daemon would violate intent.
func connectDaemonAtSocket(ctx context.Context, sockPath string) (*sdkClient, error) {
	// Share explicit target semantics with the native application connector.
	conn, err := daemon.NewConnector(connectDaemonDial, connectDaemonStart).Dial(ctx, "", sockPath)
	if err != nil {
		return nil, err
	}
	return connectDaemonBuildClient(ctx, conn)
}

// connectDaemonFromContext picks the right connection path based on CLI flags
// visible in the context lineage. If --socket-path (or its env var) is set,
// dial that socket directly. Otherwise resolve the state path and attach to or
// start its daemon socket.
func connectDaemonFromContext(ctx context.Context, c *cli.Context, statePathFallback string) (*sdkClient, error) {
	var resolved string
	if effectiveSocketPath(c, "") == "" {
		var err error
		resolved, err = resolveStatePathFromContext(c, statePathFallback)
		if err != nil {
			return nil, err
		}
	}
	return connectDaemonWithResolvedFallback(ctx, c, resolved)
}

// connectDaemonWithResolvedFallback honors --socket-path when present,
// otherwise attaches to or starts an already-resolved state path. With
// --remote-daemon, the local daemon carries the connection to the remote one.
func connectDaemonWithResolvedFallback(ctx context.Context, c *cli.Context, resolved string) (*sdkClient, error) {
	// Connect to the local daemon.
	var local *sdkClient
	var err error
	if sockPath := effectiveSocketPath(c, ""); sockPath != "" {
		local, err = connectDaemonAtSocket(ctx, sockPath)
	} else {
		local, err = connectDaemonWithAutostart(ctx, resolved)
	}
	if err != nil {
		return nil, err
	}

	// Carry the connection to the remote daemon when one is selected.
	return connectRemoteFromContext(ctx, c, local)
}

// buildSDKClient adopts the shared initialized Resource connection.
func buildSDKClient(ctx context.Context, conn net.Conn) (*sdkClient, error) {
	// The shared client owns cleanup on initialization failure and success.
	client, err := daemon.NewClient(ctx, conn)
	if err != nil {
		return nil, err
	}
	return &sdkClient{
		conn: client.Conn(), srpc: client.RPC(), resClient: client.Resources(),
		root: client.Root(), shared: client,
	}, nil
}

// buildSDKClientFromInvoker initializes an in-process Resource connection.
func buildSDKClientFromInvoker(ctx context.Context, invoker srpc.Invoker) (*sdkClient, error) {
	// Build the in-process SRPC client and resource client.
	srpcClient := srpc.NewClient(srpc.NewServerPipe(srpc.NewServer(invoker)))
	resourceSvc := resource.NewSRPCResourceServiceClient(srpcClient)
	resClient, err := resource_client.NewClient(ctx, resourceSvc)
	if err != nil {
		return nil, errors.Wrap(err, "resource client")
	}

	// Access the root resource and build the SDK root.
	rootRef := resClient.AccessRootResource()
	root, err := s4wave_root.NewRoot(resClient, rootRef)
	if err != nil {
		rootRef.Release()
		resClient.Release()
		return nil, errors.Wrap(err, "root resource")
	}

	return &sdkClient{
		srpc:      srpcClient,
		resClient: resClient,
		root:      root,
	}, nil
}

// NewClient connects the command runner to its selected daemon.
func (nativeClientFactory) NewClient(ctx context.Context, c *cli.Context) (runner.Client, error) {
	client, err := connectDaemonFromContext(ctx, c, defaultStatePath)
	if err != nil {
		return nil, err
	}
	return &nativeClient{client: client}, nil
}

// StatusEndpoint resolves the command runner's exact socket target.
func (nativeClientFactory) StatusEndpoint(ctx context.Context, c *cli.Context) (string, error) {
	return daemonSocketPath(c, defaultStatePath)
}

// Close releases this runner's connection without stopping shared work.
func (c *nativeClient) Close() {
	c.client.close()
}

// MountSession adopts a Session for the command runner.
func (c *nativeClient) MountSession(ctx context.Context, idx uint32) (runner.Session, error) {
	sess, err := c.client.mountSession(ctx, idx)
	if err != nil {
		return nil, err
	}
	return &nativeSession{Session: sess}, nil
}

// WatchResourcesList forwards the retained Session resource watch.
func (s *nativeSession) WatchResourcesList(ctx context.Context) (runner.ResourcesListStream, error) {
	return s.Session.WatchResourcesList(ctx)
}

// WatchLockState forwards Session lock changes.
func (s *nativeSession) WatchLockState(ctx context.Context) (runner.LockStateStream, error) {
	return s.Session.WatchLockState(ctx)
}

// mountSession mounts a session by index and returns the Session SDK wrapper.
func (c *sdkClient) mountSession(ctx context.Context, idx uint32) (*s4wave_session.Session, error) {
	return s4wave_mount.Session(ctx, c.resClient, c.root, idx)
}

// mountSharedObject mounts a shared object by ID and returns its
// SharedObjectResourceService client.
func (c *sdkClient) mountSharedObject(ctx context.Context, sess *s4wave_session.Session, sharedObjectID string) (s4wave_sobject.SRPCSharedObjectResourceServiceClient, func(), error) {
	return s4wave_mount.SharedObject(ctx, c.resClient, sess, sharedObjectID)
}

// mountSpace mounts a space by shared object ID and returns the SpaceResourceService client.
func (c *sdkClient) mountSpace(ctx context.Context, sess *s4wave_session.Session, sharedObjectID string) (s4wave_space.SRPCSpaceResourceServiceClient, func(), error) {
	return s4wave_mount.Space(ctx, c.resClient, sess, sharedObjectID)
}

// resolveSpaceID resolves a space argument to a shared object ID.
func (c *sdkClient) resolveSpaceID(ctx context.Context, sess *s4wave_session.Session, spaceID string) (string, error) {
	return s4wave_mount.ResolveSpaceID(ctx, sess, spaceID)
}

// getSpaceByName finds a space by name and returns its shared object ID.
// If name is empty, returns the first space found.
func (c *sdkClient) getSpaceByName(ctx context.Context, sess *s4wave_session.Session, name string) (string, error) {
	// Collect the spaces from the session's list snapshot.
	spaces, err := s4wave_mount.ListSpaces(ctx, sess)
	if err != nil {
		return "", err
	}
	if len(spaces) == 0 {
		return "", errors.New("no spaces found")
	}

	// Select the first space when no name is given, else the matching name.
	for _, sp := range spaces {
		if name == "" || sp.GetSpaceMeta().GetName() == name {
			return sp.GetEntry().GetRef().GetProviderResourceRef().GetId(), nil
		}
	}
	return "", errors.Errorf("space %q not found", name)
}

// accessWorldEngine accesses the world engine via a space's SpaceResourceService.
func (c *sdkClient) accessWorldEngine(ctx context.Context, spaceSvc s4wave_space.SRPCSpaceResourceServiceClient) (*sdk_engine.SDKEngine, func(), error) {
	engine, err := s4wave_mount.WorldEngine(ctx, c.resClient, spaceSvc)
	if err != nil {
		return nil, nil, err
	}
	return engine, engine.Release, nil
}

// close releases all resources, closes the connection, and cancels the
// Resource client context.
func (c *sdkClient) close() {
	// Free a remote client's carrier after the client itself.
	if c.release != nil {
		defer c.release()
	}

	// Delegate socket lifetimes to the shared daemon client.
	if c.shared != nil {
		c.shared.Close()
		return
	}

	// In-process clients retain only Resource references.
	if c.root != nil {
		c.root.Release()
	}
	if c.resClient != nil {
		c.resClient.Release()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}

// resolveStatePath resolves the state path, making it absolute if needed.
// For relative paths, checks cwd first, then falls back to git repo root.
func resolveStatePath(statePath string) (string, error) {
	// Return absolute state paths unchanged.
	if filepath.IsAbs(statePath) {
		return statePath, nil
	}

	// Prefer a state path under the cwd that holds a live socket.
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	cwdPath := filepath.Join(cwd, statePath)
	sockPath := filepath.Join(cwdPath, socketName)
	if _, err := os.Stat(sockPath); err == nil {
		return cwdPath, nil
	}

	// Fall back to the git repo root when it holds a live socket.
	root, err := gitroot.FindRepoRoot()
	if err == nil {
		gitPath := filepath.Join(root, statePath)
		gitSock := filepath.Join(gitPath, socketName)
		if _, err := os.Stat(gitSock); err == nil {
			return gitPath, nil
		}
	}
	return cwdPath, nil
}

// projectLocalStateDirName is the canonical project-local state directory
// name searched in cwd / git-root when no --state-path flag is specified.
// Shares the dot-prefixed project identifier used by the shared default
// state root (~/.spacewave on darwin and linux).
const projectLocalStateDirName = "." + projectID

// discoverProjectLocalStatePath returns the path of a directory in cwd or
// git-root that contains a live daemon socket under the canonical
// project-local state directory name. Dev workflows that keep a daemon
// under a project's working tree (cwd/.spacewave/spacewave.sock) take
// precedence over the shared user-level state root.
func discoverProjectLocalStatePath() (string, bool) {
	if cwd, err := os.Getwd(); err == nil {
		cwdPath := filepath.Join(cwd, projectLocalStateDirName)
		if _, err := os.Stat(filepath.Join(cwdPath, socketName)); err == nil {
			return cwdPath, true
		}
	}
	if root, err := gitroot.FindRepoRoot(); err == nil {
		gitPath := filepath.Join(root, projectLocalStateDirName)
		if _, err := os.Stat(filepath.Join(gitPath, socketName)); err == nil {
			return gitPath, true
		}
	}
	return "", false
}

// statePathUserSet reports whether --state-path was explicitly provided
// via a CLI flag or environment variable on any context in the lineage.
// When false, the shared default applies.
func statePathUserSet(c *cli.Context) bool {
	_, set := lineageFlagSet(c, "state-path")
	return set
}

// lineageFlagValue walks c.Lineage() and returns the first non-empty
// value for the named flag. found is true when a context in the
// lineage carries the flag, even if its value is empty.
func lineageFlagValue(c *cli.Context, name string) (value string, found bool) {
	if c == nil {
		return "", false
	}
	for _, ctx := range c.Lineage() {
		if !hasLocalFlag(ctx, name) {
			continue
		}
		found = true
		if v := ctx.String(name); v != "" {
			return v, true
		}
	}
	return "", found
}

// lineageFlagSet walks c.Lineage() and returns the value of the named
// flag along with whether any context in the lineage explicitly set
// the flag (via CLI argument or matching env var).
func lineageFlagSet(c *cli.Context, name string) (value string, set bool) {
	if c == nil {
		return "", false
	}
	for _, ctx := range c.Lineage() {
		if !hasLocalFlag(ctx, name) {
			continue
		}
		if ctx.IsSet(name) {
			return ctx.String(name), true
		}
	}
	return "", false
}

// resolveStatePathFromContext resolves the effective state path from CLI state.
//
// When --state-path was not explicitly set, dev workflows with a live
// daemon socket in cwd/.spacewave or git-root/.spacewave take precedence
// over the shared default. Explicit --state-path values skip project
// discovery and use relative-path resolution as before.
func resolveStatePathFromContext(c *cli.Context, fallback string) (string, error) {
	if !statePathUserSet(c) {
		if discovered, ok := discoverProjectLocalStatePath(); ok {
			return discovered, nil
		}
	}
	return resolveStatePath(effectiveStatePath(c, fallback))
}

// effectiveStatePath selects the nearest state flag or caller fallback.
func effectiveStatePath(c *cli.Context, fallback string) string {
	if value, _ := lineageFlagValue(c, "state-path"); value != "" {
		return value
	}
	if fallback != "" {
		return fallback
	}
	return defaultStatePath
}

// effectiveSocketPath returns the --socket-path value from the nearest
// CLI context that carries the flag (or its env var fallback), or the
// fallback string if none is set. An empty return value means "not set"
// and signals the caller to fall back to state-path resolution.
func effectiveSocketPath(c *cli.Context, fallback string) string {
	if value, _ := lineageFlagValue(c, "socket-path"); value != "" {
		return value
	}
	return fallback
}

// daemonSocketPath returns the socket of the daemon a command addresses: the
// absolute --socket-path value when set, else the socket in the state path.
func daemonSocketPath(c *cli.Context, statePath string) (string, error) {
	if socketPath := effectiveSocketPath(c, ""); socketPath != "" {
		if !filepath.IsAbs(socketPath) {
			return "", errors.New("daemon socket path must be absolute")
		}
		return filepath.Clean(socketPath), nil
	}
	resolved, err := resolveStatePathFromContext(c, statePath)
	if err != nil {
		return "", err
	}
	return filepath.Join(resolved, socketName), nil
}

// hasLocalFlag reports a flag defined in this command context.
func hasLocalFlag(c *cli.Context, name string) bool {
	return slices.Contains(c.LocalFlagNames(), name)
}

// mountSpaceContents mounts space contents and returns the SpaceContentsResourceService client.
func (c *sdkClient) mountSpaceContents(ctx context.Context, spaceSvc s4wave_space.SRPCSpaceResourceServiceClient) (s4wave_space.SRPCSpaceContentsResourceServiceClient, func(), error) {
	// Mount the space contents resource.
	resp, err := spaceSvc.MountSpaceContents(ctx, &s4wave_space.MountSpaceContentsRequest{})
	if err != nil {
		return nil, nil, errors.Wrap(err, "mount space contents")
	}

	// Wrap the space contents resource reference.
	ref := c.resClient.CreateResourceReference(resp.GetResourceId())
	client, err := ref.GetClient()
	if err != nil {
		ref.Release()
		return nil, nil, errors.Wrap(err, "space contents client")
	}

	// Build the service client and cleanup.
	// Wrap the space contents resource reference.
	svc := s4wave_space.NewSRPCSpaceContentsResourceServiceClient(client)
	cleanup := func() {
		ref.Release()
	}
	return svc, cleanup, nil
}

// lookupProvider accesses a provider resource by ID and returns the ProviderResourceService client.
func (c *sdkClient) lookupProvider(ctx context.Context, providerID string) (s4wave_provider.SRPCProviderResourceServiceClient, func(), error) {
	// Look up the provider resource.
	resourceID, err := c.root.LookupProvider(ctx, providerID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "lookup provider")
	}

	// Wrap the provider resource reference.
	ref := c.resClient.CreateResourceReference(resourceID)
	client, err := ref.GetClient()
	if err != nil {
		ref.Release()
		return nil, nil, errors.Wrap(err, "provider client")
	}

	// Build the service client and cleanup.
	// Wrap the provider resource reference.
	svc := s4wave_provider.NewSRPCProviderResourceServiceClient(client)
	cleanup := func() {
		ref.Release()
	}
	return svc, cleanup, nil
}

// accessAccount accesses a provider account resource by provider ID and account ID.
func (c *sdkClient) accessAccount(ctx context.Context, providerID, accountID string) (s4wave_account.SRPCAccountResourceServiceClient, func(), error) {
	// Access the provider account resource.
	providerSvc, providerCleanup, err := c.lookupProvider(ctx, providerID)
	if err != nil {
		return nil, nil, err
	}

	// Access the account resource and wrap its client.
	resp, err := providerSvc.AccessProviderAccount(ctx, &s4wave_provider.AccessProviderAccountRequest{AccountId: accountID})
	if err != nil {
		providerCleanup()
		return nil, nil, errors.Wrap(err, "access provider account")
	}

	// Wrap the account resource reference.
	ref := c.resClient.CreateResourceReference(resp.GetResourceId())
	client, err := ref.GetClient()
	if err != nil {
		ref.Release()
		providerCleanup()
		return nil, nil, errors.Wrap(err, "account client")
	}

	// Build the account service client and cleanup.
	svc := s4wave_account.NewSRPCAccountResourceServiceClient(client)
	cleanup := func() {
		ref.Release()
		providerCleanup()
	}
	return svc, cleanup, nil
}

// lookupSpacewaveProvider looks up the spacewave provider and returns an SDK wrapper.
// If providerID is empty, defaults to "spacewave".
func (c *sdkClient) lookupSpacewaveProvider(ctx context.Context, providerID string) (*s4wave_provider_spacewave.SpacewaveProvider, func(), error) {
	// Default the provider ID.
	if providerID == "" {
		providerID = "spacewave"
	}

	// Look up the provider resource.
	resourceID, err := c.root.LookupProvider(ctx, providerID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "lookup provider")
	}

	// Build the SDK wrapper over the provider resource.
	ref := c.resClient.CreateResourceReference(resourceID)
	prov, err := s4wave_provider_spacewave.NewSpacewaveProvider(c.resClient, ref)
	if err != nil {
		ref.Release()
		return nil, nil, errors.Wrap(err, "spacewave provider")
	}

	// Build the cleanup that releases the provider.
	cleanup := func() {
		prov.Release()
	}
	return prov, cleanup, nil
}

// lookupLocalProvider looks up the local provider and returns an SDK wrapper.
func (c *sdkClient) lookupLocalProvider(ctx context.Context) (*s4wave_provider_local.LocalProvider, func(), error) {
	// Look up the local provider resource.
	resourceID, err := c.root.LookupProvider(ctx, "local")
	if err != nil {
		return nil, nil, errors.Wrap(err, "lookup provider")
	}

	// Build the SDK wrapper over the provider resource.
	ref := c.resClient.CreateResourceReference(resourceID)
	prov, err := s4wave_provider_local.NewLocalProvider(c.resClient, ref)
	if err != nil {
		ref.Release()
		return nil, nil, errors.Wrap(err, "local provider")
	}

	// Build the cleanup that releases the provider.
	cleanup := func() {
		prov.Release()
	}
	return prov, cleanup, nil
}

// objectMount holds one mounted daemon-to-world-object chain and the daemon
// client it runs on.
type objectMount struct {
	*s4wave_mount.Object
	// client is the daemon client the chain was mounted on.
	client *sdkClient
}

// release unwinds the chain and closes the daemon client.
func (m *objectMount) release() {
	m.Release()
	m.client.close()
}

// mountObjectChain connects to the daemon and mounts session -> space ->
// world engine -> typed object for the URI. When resolveObjectKey is nil,
// uri.objectKey is used verbatim; otherwise it resolves the key against
// the mounted World engine. The caller releases the mount.
func mountObjectChain(
	c *cli.Context,
	statePath string,
	uri fsURI,
	resolveObjectKey func(ctx context.Context, engine *sdk_engine.SDKEngine) (string, error),
) (*objectMount, error) {
	// Connect to the daemon addressed by the command context.
	client, err := connectDaemonFromContext(c.Context, c, statePath)
	if err != nil {
		return nil, err
	}

	// Mount the chain, closing the client if it fails.
	obj, err := s4wave_mount.MountObject(c.Context, client.resClient, client.root, s4wave_mount.Target{
		SessionIdx:       uri.sessionIdx,
		Space:            uri.spaceID,
		ObjectKey:        uri.objectKey,
		ResolveObjectKey: resolveObjectKey,
	})
	if err != nil {
		client.close()
		return nil, err
	}
	return &objectMount{Object: obj, client: client}, nil
}
