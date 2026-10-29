package forge_lib_docker

import (
	"context"
	"io"
	"slices"
)

// recordedCommand retains one offline Docker invocation.
type recordedCommand struct {
	// name identifies the recorded Docker executable.
	name string
	// args retains a copy of the Docker command arguments.
	args []string
	// env retains a copy of the explicit Docker environment.
	env []string
}

// recordingRunner records Docker commands without using a daemon.
type recordingRunner struct {
	// outputs configures stdout by Docker subcommand.
	outputs map[string][]byte
	// errors configures failures by Docker subcommand.
	errors map[string]error
	// stderr configures the separate container error stream.
	stderr []byte
	// commands records calls, read after synchronous execution or its completion channel.
	commands []recordedCommand
	// waitStarted closes when the runner reaches container wait.
	waitStarted chan struct{}
	// waitCancel cancels execution after reaching the wait gate.
	waitCancel func()
}

// Stream records the command and writes its configured output and the
// separate error stream.
func (r *recordingRunner) Stream(ctx context.Context, name string, args []string, env []string, stdout, stderr io.Writer) error {
	// Record the command and write its configured streams.
	out, err := r.Run(ctx, name, args, env)
	if err != nil {
		return err
	}
	if _, err := stdout.Write(out); err != nil {
		return err
	}
	_, err = stderr.Write(r.stderr)
	return err
}

// Run records the command and applies its configured output, error, or cancellation gate.
func (r *recordingRunner) Run(ctx context.Context, name string, args []string, env []string) ([]byte, error) {
	// Retain the Docker invocation and its explicit environment.
	cmd := recordedCommand{
		name: name,
		args: slices.Clone(args),
		env:  slices.Clone(env),
	}
	r.commands = append(r.commands, cmd)
	if len(args) == 0 {
		return nil, nil
	}

	// Block container wait on the test's cancellation gate.
	if args[0] == "wait" && r.waitStarted != nil {
		close(r.waitStarted)
		r.waitCancel()
		<-ctx.Done()
		return nil, context.Canceled
	}

	// Return the configured Docker command result.
	if err := r.errors[args[0]]; err != nil {
		return nil, err
	}
	return r.outputs[args[0]], nil
}

// _ verifies the production Docker runner contract.
var _ DockerRunner = (*recordingRunner)(nil)
