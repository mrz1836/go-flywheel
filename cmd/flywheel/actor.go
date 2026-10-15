package main

import (
	"os"
	"os/user"

	flywheel "github.com/mrz1836/go-flywheel"
)

// cliSource is the source every control the CLI runs records.
const cliSource = "cli"

// operatorActor is the actor a control command runs as: the operating-system
// user, with the reason the command was given.
func operatorActor(reason string) flywheel.Actor {
	return flywheel.Actor{Type: flywheel.ActorUser, ID: osUserName(), Reason: reason, Source: cliSource}
}

// osUserName returns the operating-system user running the CLI: the current
// user's name, else $USER, else empty.
func osUserName() string {
	if u, err := user.Current(); err == nil && u.Username != "" {
		return u.Username
	}
	return os.Getenv("USER")
}
