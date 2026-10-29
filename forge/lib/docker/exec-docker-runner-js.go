//go:build js

package forge_lib_docker

import (
	"context"
	"io"

	"github.com/pkg/errors"
)

// ErrDockerUnavailable is returned by the browser runner, which cannot start
// subprocesses.
var ErrDockerUnavailable = errors.New("docker is unavailable in a browser runtime")

// ExecDockerRunner reports that a browser runtime has no docker CLI.
type ExecDockerRunner struct{}

// NewExecDockerRunner constructs the browser docker runner.
func NewExecDockerRunner() *ExecDockerRunner {
	return &ExecDockerRunner{}
}

// Run returns ErrDockerUnavailable.
func (r *ExecDockerRunner) Run(context.Context, string, []string, []string) ([]byte, error) {
	return nil, ErrDockerUnavailable
}

// Stream returns ErrDockerUnavailable.
func (r *ExecDockerRunner) Stream(context.Context, string, []string, []string, io.Writer, io.Writer) error {
	return ErrDockerUnavailable
}

// _ is a type assertion
var _ DockerRunner = (*ExecDockerRunner)(nil)
