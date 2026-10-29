//go:build !js

package forge_lib_docker

import (
	"bytes"
	"context"
	"io"
	"os/exec"
	"strings"

	"github.com/pkg/errors"
)

// ExecDockerRunner runs docker CLI commands as subprocesses with an
// explicitly constructed environment. Run returns stdout for parsing and
// includes stderr in failure errors, so Docker warnings cannot corrupt parsed
// values such as container IDs. Stream passes both streams through separately.
type ExecDockerRunner struct{}

// NewExecDockerRunner constructs a subprocess docker runner.
func NewExecDockerRunner() *ExecDockerRunner {
	return &ExecDockerRunner{}
}

// Run executes the named binary with args and env, returning stdout.
func (r *ExecDockerRunner) Run(ctx context.Context, name string, args []string, env []string) ([]byte, error) {
	var stdout, stderr bytes.Buffer
	err := r.Stream(ctx, name, args, env, &stdout, &stderr)
	if err != nil && ctx.Err() == nil {
		err = errors.Errorf("%s: %s", err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), err
}

// Stream executes the named binary with args and env, copying its output
// streams to stdout and stderr as it writes them.
func (r *ExecDockerRunner) Stream(ctx context.Context, name string, args []string, env []string, stdout, stderr io.Writer) error {
	// Run the command with only the given environment and writers.
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = env
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return context.Canceled
		}
		return errors.Errorf("%s %s failed: %s", name, strings.Join(args, " "), err)
	}
	return nil
}

// _ is a type assertion
var _ DockerRunner = (*ExecDockerRunner)(nil)
