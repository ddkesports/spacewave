package forge_lib_docker

import (
	"context"
	"io"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// Container is one Docker container, addressed by its ID through the CLI path
// and environment of the Config that created it.
type Container struct {
	// runner executes the CLI.
	runner DockerRunner
	// path is the Docker CLI executable.
	path string
	// env is the complete Docker CLI environment.
	env []string
	// id is the container ID docker create returned.
	id string
}

// CreateContainer creates a stopped container for conf named name, which may
// be empty to let Docker choose. A nonempty outputDir is the host directory
// mounted at conf's output_dir.
func CreateContainer(ctx context.Context, runner DockerRunner, conf *Config, name, outputDir string) (*Container, error) {
	// Resolve the CLI the config selects.
	c := &Container{runner: runner, path: conf.GetDockerPath(), env: BuildDockerEnv(conf)}
	if c.path == "" {
		c.path = "docker"
	}

	// Create the container and keep the ID it prints.
	out, err := c.runner.Run(ctx, c.path, buildCreateArgs(conf, name, outputDir), c.env)
	if err != nil {
		return nil, errors.Wrap(err, "docker create")
	}
	c.id = strings.TrimSpace(string(out))
	if c.id == "" {
		return nil, errors.New("docker create returned empty container id")
	}
	return c, nil
}

// GetID returns the container ID.
func (c *Container) GetID() string {
	return c.id
}

// Start starts the container.
func (c *Container) Start(ctx context.Context) error {
	_, err := c.runner.Run(ctx, c.path, []string{"start", c.id}, c.env)
	return errors.Wrap(err, "docker start")
}

// Wait waits for the container to exit and returns its exit status.
func (c *Container) Wait(ctx context.Context) (int, error) {
	out, err := c.runner.Run(ctx, c.path, []string{"wait", c.id}, c.env)
	if err != nil {
		return 0, errors.Wrap(err, "docker wait")
	}
	status, err := strconv.Atoi(strings.TrimSpace(string(out)))
	return status, errors.Wrap(err, "parse docker wait status")
}

// Logs copies the container's output streams to stdout and stderr. With
// follow it keeps copying until the container stops or ctx ends; without it
// returns after the output written so far.
func (c *Container) Logs(ctx context.Context, follow bool, stdout, stderr io.Writer) error {
	args := []string{"logs"}
	if follow {
		args = append(args, "--follow")
	}
	args = append(args, c.id)
	return errors.Wrap(c.runner.Stream(ctx, c.path, args, c.env, stdout, stderr), "docker logs")
}

// Remove stops the container if it is running and removes it.
func (c *Container) Remove(ctx context.Context) error {
	_, err := c.runner.Run(ctx, c.path, []string{"rm", "--force", c.id}, c.env)
	return errors.Wrap(err, "docker rm")
}
