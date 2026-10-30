//go:build !js

package spacewave_cli

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/aperturerobotics/cli"
	"github.com/aperturerobotics/fastjson"
	"github.com/pkg/errors"
	cli_entrypoint "github.com/s4wave/spacewave/bldr/cli/entrypoint"
	"github.com/s4wave/spacewave/db/world"
	s4wave_k8s "github.com/s4wave/spacewave/sdk/k8s"
	s4wave_space "github.com/s4wave/spacewave/sdk/space"
)

// k8sArgs selects the daemon, session and Space of a k8s command.
type k8sArgs struct {
	statePath  string
	spaceID    string
	sessionIdx int
}

// BuildFlags returns the flags shared by every k8s subcommand.
func (a *k8sArgs) BuildFlags() []cli.Flag {
	return []cli.Flag{
		statePathFlag(&a.statePath),
		socketPathFlag(),
		&cli.StringFlag{
			Name:        "space",
			Usage:       "Space ID or name (default: the only Space)",
			EnvVars:     []string{"SPACEWAVE_SPACE"},
			Destination: &a.spaceID,
		},
		&cli.IntFlag{
			Name:        "session-index",
			Usage:       "session index",
			EnvVars:     []string{"SPACEWAVE_SESSION_INDEX"},
			Value:       1,
			Destination: &a.sessionIdx,
		},
	}
}

// newK8sCommand builds the k8s command group.
func newK8sCommand(_ func() cli_entrypoint.CliBus) *cli.Command {
	return &cli.Command{
		Name:  "k8s",
		Usage: "serve a Space's Kubernetes API to kubectl with the spacewave-k8s plugin",
		Subcommands: []*cli.Command{
			buildK8sBindCommand(true),
			buildK8sBindCommand(false),
			buildK8sKubeconfigCommand(),
			buildK8sCredentialCommand(),
		},
	}
}

// k8sBindArgs turns the Space's Kubernetes API on or off on this Device.
type k8sBindArgs struct {
	k8sArgs
	enable bool
}

// Run creates the Space's k8s/cluster object when enabling, then records
// this Device's binding decision for it.
func (a *k8sBindArgs) Run(c *cli.Context) error {
	idx, err := sessionIndexFromInt(a.sessionIdx)
	if err != nil {
		return err
	}
	return withVmSpace(c, a.statePath, idx, a.spaceID, func(client *sdkClient, spaceSvc s4wave_space.SRPCSpaceResourceServiceClient, engine world.Engine) error {
		// Create the cluster object.
		if a.enable {
			err := world.ExecTransaction(c.Context, engine, true, func(ctx context.Context, tx world.WorldState) error {
				return s4wave_k8s.EnsureCluster(ctx, tx)
			})
			if err != nil {
				return err
			}
		}

		// Set the binding through the Space contents resource.
		contents, release, err := client.mountSpaceContents(c.Context, spaceSvc)
		if err != nil {
			return err
		}
		defer release()
		_, err = contents.SetProcessBinding(c.Context, &s4wave_space.SetProcessBindingRequest{
			ObjectKey: s4wave_k8s.ClusterObjectKey,
			TypeId:    s4wave_k8s.ClusterTypeID,
			Approved:  a.enable,
		})
		return errors.Wrap(err, "set cluster process binding")
	})
}

// buildK8sBindCommand builds the enable or disable subcommand.
func buildK8sBindCommand(enable bool) *cli.Command {
	args := &k8sBindArgs{enable: enable}
	name, usage := "enable", "serve the Space's Kubernetes API from this Device"
	if !enable {
		name, usage = "disable", "stop serving the Space's Kubernetes API from this Device"
	}
	return &cli.Command{
		Name:   name,
		Usage:  usage,
		Flags:  args.BuildFlags(),
		Action: args.Run,
	}
}

// getAccess returns the caller's access to the Space's Kubernetes API on this
// Device and the ID of the Space.
func (a *k8sArgs) getAccess(c *cli.Context) (*s4wave_k8s.GetAccessResponse, string, error) {
	// Mount the Space's cluster object.
	idx, err := sessionIndexFromInt(a.sessionIdx)
	if err != nil {
		return nil, "", err
	}
	mount, err := mountObjectChain(c, a.statePath, fsURI{sessionIdx: idx, spaceID: a.spaceID, objectKey: s4wave_k8s.ClusterObjectKey}, nil)
	if err != nil {
		return nil, "", errors.Wrap(err, "open the Space's k8s/cluster object (enable it with spacewave k8s enable)")
	}
	defer mount.release()

	// Ask it for the server and a token.
	access, err := s4wave_k8s.NewSRPCClusterResourceServiceClient(mount.TypedClient).GetAccess(c.Context, &s4wave_k8s.GetAccessRequest{})
	return access, mount.SpaceID, err
}

// k8sKubeconfigArgs prints a kubeconfig for the Space.
type k8sKubeconfigArgs struct {
	k8sArgs
}

// Run prints a kubeconfig with one context for the Space, whose user runs
// spacewave k8s credential for its token.
func (a *k8sKubeconfigArgs) Run(c *cli.Context) error {
	// Read the server.
	access, spaceID, err := a.getAccess(c)
	if err != nil {
		return err
	}

	// Name this program and daemon in the credential command, since kubectl
	// runs it from anywhere.
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	args := []string{"k8s", "credential", "--space", spaceID, "--session-index", strconv.Itoa(a.sessionIdx)}
	flag, path := "--socket-path", effectiveSocketPath(c, "")
	if path == "" {
		flag = "--state-path"
		path, err = resolveStatePathFromContext(c, a.statePath)
		if err != nil {
			return err
		}
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return err
	}
	args = append(args, flag, path)

	// Print the kubeconfig. JSON strings and arrays are YAML ones.
	var arena fastjson.Arena
	name := "spacewave-" + spaceID
	q := func(s string) string {
		return string(arena.NewString(s).MarshalTo(nil))
	}
	quotedArgs := arena.NewArray()
	for i, arg := range args {
		quotedArgs.SetArrayItem(i, arena.NewString(arg))
	}
	_, err = fmt.Fprintf(os.Stdout, `apiVersion: v1
kind: Config
clusters:
- name: %[1]s
  cluster:
    server: %[2]s
    certificate-authority-data: %[5]s
users:
- name: %[1]s
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: %[3]s
      args: %[4]s
      interactiveMode: Never
contexts:
- name: %[1]s
  context:
    cluster: %[1]s
    user: %[1]s
current-context: %[1]s
`, q(name), q(access.GetServer()), q(exe), quotedArgs.MarshalTo(nil), q(base64.StdEncoding.EncodeToString(access.GetCertificateAuthority())))
	return err
}

// buildK8sKubeconfigCommand builds the kubeconfig subcommand.
func buildK8sKubeconfigCommand() *cli.Command {
	args := &k8sKubeconfigArgs{}
	return &cli.Command{
		Name:  "kubeconfig",
		Usage: "print a kubeconfig that reaches the Space's Kubernetes API on this Device",
		Description: "Print a kubeconfig with one context for the Space, named spacewave-<space-id>.\n" +
			"Its user runs spacewave k8s credential for a token, so the file holds no\n" +
			"secret. The Space's Kubernetes API must be enabled on this Device.\n\n" +
			"EXAMPLES:\n" +
			"  spacewave k8s enable --space my-space\n" +
			"  spacewave k8s kubeconfig --space my-space > ~/.kube/spacewave\n" +
			"  KUBECONFIG=~/.kube/spacewave kubectl get nodes",
		Flags:  args.BuildFlags(),
		Action: args.Run,
	}
}

// k8sCredentialArgs prints the caller's token as a kubectl ExecCredential.
type k8sCredentialArgs struct {
	k8sArgs
}

// Run prints the ExecCredential.
func (a *k8sCredentialArgs) Run(c *cli.Context) error {
	// Read the token.
	access, _, err := a.getAccess(c)
	if err != nil {
		return err
	}

	// Build it in the client.authentication.k8s.io/v1 shape.
	var arena fastjson.Arena
	status := arena.NewObject()
	status.Set("token", arena.NewString(access.GetToken()))
	cred := arena.NewObject()
	cred.Set("apiVersion", arena.NewString("client.authentication.k8s.io/v1"))
	cred.Set("kind", arena.NewString("ExecCredential"))
	cred.Set("status", status)

	// Print it.
	_, err = os.Stdout.Write(append(cred.MarshalTo(nil), '\n'))
	return err
}

// buildK8sCredentialCommand builds the credential subcommand.
func buildK8sCredentialCommand() *cli.Command {
	args := &k8sCredentialArgs{}
	return &cli.Command{
		Name:   "credential",
		Usage:  "print a token for the Space's Kubernetes API, for kubectl's exec credential",
		Hidden: true,
		Flags:  args.BuildFlags(),
		Action: args.Run,
	}
}
